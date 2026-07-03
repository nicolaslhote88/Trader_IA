#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Seed 100 valeurs mondiales dans ag2_v3.duckdb -> table `universe`.
- Idempotent : PRIMARY KEY (symbol) + INSERT ... ON CONFLICT (symbol) DO NOTHING.
- Ecrire IMPERATIVEMENT avec duckdb==1.4.4 (version la plus basse parmi les lecteurs
  du systeme : yf-enrichment) afin de NE PAS upgrader le format de stockage et de
  rester lisible par tous les agents.
- symbol == symbol_yahoo (coherent avec les lignes existantes).
- sector : labels Yahoo exacts deja utilises dans la base.
Usage:
    /tmp/ddb144/bin/python seed_universe_100_global.py --db /local-files/duckdb/ag2_v3.duckdb [--dry-run]
"""
import argparse, datetime, sys
import duckdb

# (symbol, name, exchange, currency, country, sector, industry)
ROWS = [
    # ===== USA (30) =====
    ("BRK-B", "Berkshire Hathaway", "NYSE", "USD", "United States", "Financial Services", "Insurance - Diversified"),
    ("UNH",  "UnitedHealth Group", "NYSE", "USD", "United States", "Healthcare", "Healthcare Plans"),
    ("JNJ",  "Johnson & Johnson", "NYSE", "USD", "United States", "Healthcare", "Drug Manufacturers - General"),
    ("PG",   "Procter & Gamble", "NYSE", "USD", "United States", "Consumer Defensive", "Household & Personal Products"),
    ("HD",   "Home Depot", "NYSE", "USD", "United States", "Consumer Cyclical", "Home Improvement Retail"),
    ("MA",   "Mastercard", "NYSE", "USD", "United States", "Financial Services", "Credit Services"),
    ("COST", "Costco Wholesale", "NASDAQ", "USD", "United States", "Consumer Defensive", "Discount Stores"),
    ("KO",   "Coca-Cola", "NYSE", "USD", "United States", "Consumer Defensive", "Beverages - Non-Alcoholic"),
    ("PEP",  "PepsiCo", "NASDAQ", "USD", "United States", "Consumer Defensive", "Beverages - Non-Alcoholic"),
    ("ABBV", "AbbVie", "NYSE", "USD", "United States", "Healthcare", "Drug Manufacturers - General"),
    ("MRK",  "Merck & Co", "NYSE", "USD", "United States", "Healthcare", "Drug Manufacturers - General"),
    ("CVX",  "Chevron", "NYSE", "USD", "United States", "Energy", "Oil & Gas Integrated"),
    ("BAC",  "Bank of America", "NYSE", "USD", "United States", "Financial Services", "Banks - Diversified"),
    ("CRM",  "Salesforce", "NYSE", "USD", "United States", "Technology", "Software - Application"),
    ("NFLX", "Netflix", "NASDAQ", "USD", "United States", "Communication Services", "Entertainment"),
    ("AMD",  "Advanced Micro Devices", "NASDAQ", "USD", "United States", "Technology", "Semiconductors"),
    ("ADBE", "Adobe", "NASDAQ", "USD", "United States", "Technology", "Software - Infrastructure"),
    ("MCD",  "McDonald's", "NYSE", "USD", "United States", "Consumer Cyclical", "Restaurants"),
    ("CAT",  "Caterpillar", "NYSE", "USD", "United States", "Industrials", "Farm & Heavy Construction Machinery"),
    ("GE",   "GE Aerospace", "NYSE", "USD", "United States", "Industrials", "Aerospace & Defense"),
    ("BA",   "Boeing", "NYSE", "USD", "United States", "Industrials", "Aerospace & Defense"),
    ("DIS",  "Walt Disney", "NYSE", "USD", "United States", "Communication Services", "Entertainment"),
    ("NKE",  "Nike", "NYSE", "USD", "United States", "Consumer Cyclical", "Footwear & Accessories"),
    ("QCOM", "Qualcomm", "NASDAQ", "USD", "United States", "Technology", "Semiconductors"),
    ("TXN",  "Texas Instruments", "NASDAQ", "USD", "United States", "Technology", "Semiconductors"),
    ("PFE",  "Pfizer", "NYSE", "USD", "United States", "Healthcare", "Drug Manufacturers - General"),
    ("VZ",   "Verizon Communications", "NYSE", "USD", "United States", "Communication Services", "Telecom Services"),
    ("UNP",  "Union Pacific", "NYSE", "USD", "United States", "Industrials", "Railroads"),
    ("GS",   "Goldman Sachs", "NYSE", "USD", "United States", "Financial Services", "Capital Markets"),
    ("UBER", "Uber Technologies", "NYSE", "USD", "United States", "Technology", "Software - Application"),
    # ===== Europe hors-France (25) =====
    ("SAP",    "SAP SE (ADR)", "NYSE", "USD", "Germany", "Technology", "Software - Application"),
    ("SIE.DE", "Siemens AG", "XETRA", "EUR", "Germany", "Industrials", "Specialty Industrial Machinery"),
    ("ALV.DE", "Allianz SE", "XETRA", "EUR", "Germany", "Financial Services", "Insurance - Diversified"),
    ("MBG.DE", "Mercedes-Benz Group", "XETRA", "EUR", "Germany", "Consumer Cyclical", "Auto Manufacturers"),
    ("BAS.DE", "BASF SE", "XETRA", "EUR", "Germany", "Basic Materials", "Specialty Chemicals"),
    ("RHM.DE", "Rheinmetall AG", "XETRA", "EUR", "Germany", "Industrials", "Aerospace & Defense"),
    ("ASML",   "ASML Holding (ADR)", "NASDAQ", "USD", "Netherlands", "Technology", "Semiconductor Equipment & Materials"),
    ("PRX.AS", "Prosus NV", "Euronext Amsterdam", "EUR", "Netherlands", "Communication Services", "Internet Content & Information"),
    ("AD.AS",  "Ahold Delhaize", "Euronext Amsterdam", "EUR", "Netherlands", "Consumer Defensive", "Grocery Stores"),
    ("NESN.SW","Nestle SA", "SIX", "CHF", "Switzerland", "Consumer Defensive", "Packaged Foods"),
    ("ROG.SW", "Roche Holding", "SIX", "CHF", "Switzerland", "Healthcare", "Drug Manufacturers - General"),
    ("NVS",    "Novartis AG (ADR)", "NYSE", "USD", "Switzerland", "Healthcare", "Drug Manufacturers - General"),
    ("UBS",    "UBS Group", "NYSE", "USD", "Switzerland", "Financial Services", "Banks - Diversified"),
    ("ABB",    "ABB Ltd (ADR)", "NYSE", "USD", "Switzerland", "Industrials", "Electrical Equipment & Parts"),
    ("AZN",    "AstraZeneca (ADR)", "NASDAQ", "USD", "United Kingdom", "Healthcare", "Drug Manufacturers - General"),
    ("SHEL",   "Shell plc (ADR)", "NYSE", "USD", "United Kingdom", "Energy", "Oil & Gas Integrated"),
    ("HSBC",   "HSBC Holdings (ADR)", "NYSE", "USD", "United Kingdom", "Financial Services", "Banks - Diversified"),
    ("UL",     "Unilever (ADR)", "NYSE", "USD", "United Kingdom", "Consumer Defensive", "Household & Personal Products"),
    ("RIO",    "Rio Tinto (ADR)", "NYSE", "USD", "United Kingdom", "Basic Materials", "Other Industrial Metals & Mining"),
    ("BP",     "BP plc (ADR)", "NYSE", "USD", "United Kingdom", "Energy", "Oil & Gas Integrated"),
    ("DEO",    "Diageo (ADR)", "NYSE", "USD", "United Kingdom", "Consumer Defensive", "Beverages - Wineries & Distilleries"),
    ("ITX.MC", "Inditex", "BME", "EUR", "Spain", "Consumer Cyclical", "Apparel Retail"),
    ("SAN",    "Banco Santander (ADR)", "NYSE", "USD", "Spain", "Financial Services", "Banks - Diversified"),
    ("RACE",   "Ferrari NV", "NYSE", "USD", "Italy", "Consumer Cyclical", "Auto Manufacturers"),
    ("NVO",    "Novo Nordisk (ADR)", "NYSE", "USD", "Denmark", "Healthcare", "Drug Manufacturers - General"),
    # ===== Asie developpee (23) =====
    ("TM",       "Toyota Motor (ADR)", "NYSE", "USD", "Japan", "Consumer Cyclical", "Auto Manufacturers"),
    ("SONY",     "Sony Group (ADR)", "NYSE", "USD", "Japan", "Technology", "Consumer Electronics"),
    ("MUFG",     "Mitsubishi UFJ Financial (ADR)", "NYSE", "USD", "Japan", "Financial Services", "Banks - Diversified"),
    ("8035.T",   "Tokyo Electron", "Tokyo", "JPY", "Japan", "Technology", "Semiconductor Equipment & Materials"),
    ("6861.T",   "Keyence", "Tokyo", "JPY", "Japan", "Technology", "Scientific & Technical Instruments"),
    ("9984.T",   "SoftBank Group", "Tokyo", "JPY", "Japan", "Communication Services", "Telecom Services"),
    ("7974.T",   "Nintendo", "Tokyo", "JPY", "Japan", "Communication Services", "Electronic Gaming & Multimedia"),
    ("6501.T",   "Hitachi", "Tokyo", "JPY", "Japan", "Industrials", "Specialty Industrial Machinery"),
    ("9983.T",   "Fast Retailing", "Tokyo", "JPY", "Japan", "Consumer Cyclical", "Apparel Retail"),
    ("4063.T",   "Shin-Etsu Chemical", "Tokyo", "JPY", "Japan", "Basic Materials", "Specialty Chemicals"),
    ("6098.T",   "Recruit Holdings", "Tokyo", "JPY", "Japan", "Industrials", "Staffing & Employment Services"),
    ("8058.T",   "Mitsubishi Corp", "Tokyo", "JPY", "Japan", "Industrials", "Conglomerates"),
    ("005930.KS","Samsung Electronics", "KRX", "KRW", "South Korea", "Technology", "Semiconductors"),
    ("000660.KS","SK Hynix", "KRX", "KRW", "South Korea", "Technology", "Semiconductors"),
    ("TSM",      "Taiwan Semiconductor (ADR)", "NYSE", "USD", "Taiwan", "Technology", "Semiconductors"),
    ("BHP",      "BHP Group (ADR)", "NYSE", "USD", "Australia", "Basic Materials", "Other Industrial Metals & Mining"),
    ("CBA.AX",   "Commonwealth Bank of Australia", "ASX", "AUD", "Australia", "Financial Services", "Banks - Diversified"),
    ("CSL.AX",   "CSL Limited", "ASX", "AUD", "Australia", "Healthcare", "Drug Manufacturers - Specialty & Generic"),
    ("WDS.AX",   "Woodside Energy", "ASX", "AUD", "Australia", "Energy", "Oil & Gas E&P"),
    ("MQG.AX",   "Macquarie Group", "ASX", "AUD", "Australia", "Financial Services", "Capital Markets"),
    ("WES.AX",   "Wesfarmers", "ASX", "AUD", "Australia", "Consumer Cyclical", "Specialty Retail"),
    ("D05.SI",   "DBS Group", "SGX", "SGD", "Singapore", "Financial Services", "Banks - Regional"),
    ("O39.SI",   "Oversea-Chinese Banking (OCBC)", "SGX", "SGD", "Singapore", "Financial Services", "Banks - Regional"),
    # ===== Marches emergents (14) =====
    ("BABA",   "Alibaba Group (ADR)", "NYSE", "USD", "China", "Consumer Cyclical", "Internet Retail"),
    ("0700.HK","Tencent Holdings", "HKEX", "HKD", "China", "Communication Services", "Internet Content & Information"),
    ("3690.HK","Meituan", "HKEX", "HKD", "China", "Consumer Cyclical", "Internet Retail"),
    ("1810.HK","Xiaomi", "HKEX", "HKD", "China", "Technology", "Consumer Electronics"),
    ("1211.HK","BYD Company", "HKEX", "HKD", "China", "Consumer Cyclical", "Auto Manufacturers"),
    ("PDD",    "PDD Holdings (ADR)", "NASDAQ", "USD", "China", "Consumer Cyclical", "Internet Retail"),
    ("JD",     "JD.com (ADR)", "NASDAQ", "USD", "China", "Consumer Cyclical", "Internet Retail"),
    ("INFY",   "Infosys (ADR)", "NYSE", "USD", "India", "Technology", "Information Technology Services"),
    ("IBN",    "ICICI Bank (ADR)", "NYSE", "USD", "India", "Financial Services", "Banks - Regional"),
    ("HDB",    "HDFC Bank (ADR)", "NYSE", "USD", "India", "Financial Services", "Banks - Regional"),
    ("VALE",   "Vale SA (ADR)", "NYSE", "USD", "Brazil", "Basic Materials", "Other Industrial Metals & Mining"),
    ("PBR",    "Petrobras (ADR)", "NYSE", "USD", "Brazil", "Energy", "Oil & Gas Integrated"),
    ("ITUB",   "Itau Unibanco (ADR)", "NYSE", "USD", "Brazil", "Financial Services", "Banks - Regional"),
    ("FMX",    "Fomento Economico Mexicano FEMSA (ADR)", "NYSE", "USD", "Mexico", "Consumer Defensive", "Beverages - Non-Alcoholic"),
    # ===== Canada (8) — listings NYSE en USD =====
    ("RY",   "Royal Bank of Canada", "NYSE", "USD", "Canada", "Financial Services", "Banks - Diversified"),
    ("TD",   "Toronto-Dominion Bank", "NYSE", "USD", "Canada", "Financial Services", "Banks - Diversified"),
    ("ENB",  "Enbridge", "NYSE", "USD", "Canada", "Energy", "Oil & Gas Midstream"),
    ("CNQ",  "Canadian Natural Resources", "NYSE", "USD", "Canada", "Energy", "Oil & Gas E&P"),
    ("CNI",  "Canadian National Railway", "NYSE", "USD", "Canada", "Industrials", "Railroads"),
    ("SHOP", "Shopify", "NYSE", "USD", "Canada", "Technology", "Software - Application"),
    ("BN",   "Brookfield Corporation", "NYSE", "USD", "Canada", "Financial Services", "Asset Management"),
    ("SU",   "Suncor Energy", "NYSE", "USD", "Canada", "Energy", "Oil & Gas Integrated"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/local-files/duckdb/ag2_v3.duckdb")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    assert len(ROWS) == 100, f"attendu 100 lignes, trouve {len(ROWS)}"
    syms = [r[0] for r in ROWS]
    assert len(set(syms)) == 100, "doublons dans la liste a inserer"

    now = datetime.datetime.utcnow()
    payload = [
        (sym, name, "EQUITY", exch, ccy, country, sector, industry,
         "", True, "", now, sym, None, None, None, None, "")
        for (sym, name, exch, ccy, country, sector, industry) in ROWS
    ]

    if args.dry_run:
        print(f"[dry-run] {len(payload)} lignes pretes, db={args.db}")
        return

    con = duckdb.connect(args.db)  # read-write
    before = con.execute("SELECT count(*) FROM universe").fetchone()[0]
    con.executemany(
        """
        INSERT INTO universe
          (symbol, name, asset_class, exchange, currency, country, sector, industry,
           isin, enabled, boursorama_ref, updated_at, symbol_yahoo,
           base_ccy, quote_ccy, pip_size, price_decimals, trading_hours)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT (symbol) DO NOTHING
        """,
        payload,
    )
    con.commit()
    after = con.execute("SELECT count(*) FROM universe").fetchone()[0]
    inserted = con.execute(
        "SELECT count(*) FROM universe WHERE symbol IN ({})".format(
            ",".join("?" * len(syms))
        ),
        syms,
    ).fetchone()[0]
    con.close()
    print(f"OK db={args.db}  before={before}  after={after}  delta={after-before}  present_du_lot={inserted}/100")


if __name__ == "__main__":
    sys.exit(main())
