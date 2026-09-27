#!/bin/sh
set -eu
exec /opt/trader-ia/finnhub/venv/bin/python /opt/trader-ia/finnhub/run_finnhub.py --segments HELD,CORE_MANUAL,CORE_AUTO --days 2 --max-per-symbol 12 --target staging >> /opt/trader-ia/finnhub/collector.log 2>&1
