#!/usr/bin/env python3
"""Load only the existing Finnhub token. Never execute the shared .env as shell code."""
import os
from pathlib import Path
from finnhub_news_collector import main

if __name__ == '__main__':
    if not os.environ.get('FINNHUB_TOKEN'):
        for line in Path('/docker/yfinance/.env').read_text().splitlines():
            key, sep, value = line.partition('=')
            if sep and key.strip() == 'FINNHUB_TOKEN':
                os.environ['FINNHUB_TOKEN'] = value.strip().strip('\"\'')
                break
    main()
