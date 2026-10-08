#!/bin/sh
set -eu
# UTC host cron invokes hourly. DST-safe decision uses Europe/Paris.
HOUR=$(TZ=Europe/Paris date +%H)
DAY=$(TZ=Europe/Paris date +%u)
[ "$DAY" -le 5 ] || exit 0
case "$HOUR" in 09|12|15) ;; *) exit 0 ;; esac
exec 9>/opt/trader-ia/finnhub/free-news.lock
flock -n 9 || exit 0
cd /opt/trader-ia/finnhub
# Existing wrapper loads the credential without printing it.
failed=0
./run_collector.sh || failed=1
./venv/bin/python issuer_news_collector.py || failed=1
exit "$failed"
