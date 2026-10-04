#!/bin/sh
# Crontab invokes at :45 hourly; runner checks Europe/Paris == 04.
# Collector lock prevents overlap with an initial backfill or evaluation.
exec docker exec ag3-predictive python runner.py --scheduled
