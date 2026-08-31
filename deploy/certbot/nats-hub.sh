#!/bin/sh
# Deploy-хук certbot: кладёт свежий сертификат туда, где его читает
# nats-server, и просит хаб перечитать конфигурацию.
#
#   sudo install -m 755 deploy/certbot/nats-hub.sh \
#       /etc/letsencrypt/renewal-hooks/deploy/10-nats-hub.sh
#
# ЗАЧЕМ ОТДЕЛЬНАЯ КОПИЯ, А НЕ ЧТЕНИЕ ИЗ /etc/letsencrypt НАПРЯМУЮ.
#
# nats-server работает не от root и приватный ключ прочитать не может:
# certbot кладёт его 0600 root:root. Напрашивается разовый `chown -R` на
# /etc/letsencrypt, и именно так предлагала прежняя версия runbook. Это
# ломается на 60-й день: при продлении certbot создаёт файлы в archive
# заново, от root, и переставляет symlink в live. Права, выставленные
# руками один раз, к новым файлам не относятся.
#
# Хуже всего, КОГДА это обнаруживается. Работающий сервер уже держит
# открытые дескрипторы и продолжает отдавать старый сертификат, пока его
# не перезапустят. То есть отказ проявляется не в момент поломки, а при
# следующем рестарте хаба — через недели, без видимой связи с продлением.
#
# Копия снимает обе проблемы: /etc/letsencrypt остаётся нетронутым, а
# сервер читает каталог, которым владеет.
#
# ПРО RENEWED_LINEAGE. Certbot передаёт deploy-хуку путь к обновлённому
# набору (/etc/letsencrypt/live/<домен>). Берём домен оттуда, а не зашиваем
# в скрипт: зашитый разъедется с конфигом молча, и хук будет обновлять
# сертификат, которым никто не пользуется.
#
# При ручном первом запуске переменной нет — тогда путь можно передать
# аргументом:
#
#   sudo /etc/letsencrypt/renewal-hooks/deploy/10-nats-hub.sh \
#       /etc/letsencrypt/live/mesh.example.com

set -eu

LINEAGE="${RENEWED_LINEAGE:-${1:-}}"
if [ -z "$LINEAGE" ]; then
	echo "нет пути к сертификату: certbot задаёт RENEWED_LINEAGE," \
		"при ручном запуске передайте его аргументом" >&2
	exit 1
fi
if [ ! -f "$LINEAGE/fullchain.pem" ] || [ ! -f "$LINEAGE/privkey.pem" ]; then
	echo "в $LINEAGE нет fullchain.pem или privkey.pem" >&2
	exit 1
fi

TLS_DIR=/etc/nats/tls
mkdir -p "$TLS_DIR"
chown root:nats "$TLS_DIR"
chmod 750 "$TLS_DIR"

# Цепочку читает кто угодно, приватный ключ — только сам сервер.
install -o nats -g nats -m 644 "$LINEAGE/fullchain.pem" "$TLS_DIR/fullchain.pem"
install -o nats -g nats -m 600 "$LINEAGE/privkey.pem" "$TLS_DIR/privkey.pem"

# Reload, а не restart: nats-server перечитывает конфигурацию и сертификаты
# по SIGHUP, не разрывая соединений узлов. Проверка на is-active нужна для
# первого запуска, когда юнита ещё нет: хук не должен падать на установке.
if systemctl is-active --quiet nats-hub; then
	systemctl reload nats-hub
fi
