#!/usr/bin/env bash
# check_rss_feeds.sh — valide une liste d'URLs RSS/Atom candidates pour AG4.
#
# Usage :
#   bash check_rss_feeds.sh                       # utilise la liste interne
#   bash check_rss_feeds.sh urls.txt              # une URL par ligne (lignes vides + # ignorées)
#   bash check_rss_feeds.sh - < urls.txt          # depuis stdin
#
# Sortie (TSV) sur stdout :
#   URL  STATUS_HTTP  CONTENT_TYPE  IS_XML  ITEM_COUNT  LATEST_PUB  VERDICT
#
# VERDICT :
#   OK_FRESH      = répond 2xx, XML, items présents, dernière pub < 30 jours
#   OK_STALE      = répond 2xx, XML, items présents, dernière pub > 30 jours
#   OK_NO_DATE    = répond 2xx, XML, items présents, pas de pubDate parsable
#   EMPTY         = répond 2xx, XML, mais pas d'items
#   NOT_XML       = répond 2xx mais pas de XML (HTML, JSON, autre)
#   HTTP_<code>   = code HTTP non-2xx (403, 404, 5xx, etc.)
#   TIMEOUT       = pas de réponse en 10s
#   DNS_ERROR     = domaine inconnu
#
# Prérequis : curl, xmllint (libxml2-utils), date GNU.

set -uo pipefail

UA="Mozilla/5.0 (compatible; Trader_IA-RSS-Checker/1.0; +https://github.com/nicolas/Trader_IA)"
TIMEOUT=10
NOW_EPOCH=$(date -u +%s)

# Liste interne (utilisée si aucun argument) — extrait du livrable AG4 RSS_CANDIDATES.md
DEFAULT_URLS=(
  # ─── Macro / Banques centrales ───
  "https://www.ecb.europa.eu/rss/press.html"
  "https://www.ecb.europa.eu/rss/fie.html"
  "https://www.bankingsupervision.europa.eu/rss/news.html"
  "https://www.federalreserve.gov/feeds/press_all.xml"
  "https://www.federalreserve.gov/feeds/press_monetary.xml"
  "https://www.federalreserve.gov/feeds/speeches.xml"
  "https://www.bankofengland.co.uk/rss/news"
  "https://www.bankofengland.co.uk/rss/publications"
  "https://www.boj.or.jp/en/rss/whatsnew.xml"
  "https://www.snb.ch/public/en/rss/news"
  "https://www.bankofcanada.ca/rss-feeds/press/"
  "https://www.bundesbank.de/service/rss/en/633286/feed.rss"
  "https://www.bis.org/list/all/index.rss"
  "https://www.bis.org/list/cbspeeches/index.rss"
  # ─── Presse financière FR ───
  "https://www.challenges.fr/economie/rss.xml"
  "https://www.challenges.fr/entreprise/rss.xml"
  "https://feed.prismamediadigital.com/v1/cap/rss?sources=capital,polemik,xerfi,capital-avec-agence-france-presse,capital-avec-aof,capital-avec-reuters&categories=entreprises-marches"
  "https://www.lefigaro.fr/rss/figaro_flash-eco.xml"
  "https://www.lefigaro.fr/rss/figaro_economie.xml"
  "https://www.lemonde.fr/economie/rss_full.xml"
  "https://www.lemonde.fr/economie-mondiale/rss_full.xml"
  "https://bfmbusiness.bfmtv.com/rss/info/flux-rss/flux-toutes-les-actualites/"
  # ─── Forex / marchés ───
  "https://www.fxstreet.com/rss/news"
  "https://www.fxstreet.com/rss/analysis"
  # ─── Sectoriels FR ───
  "https://www.batiactu.com/rss/index.php"
  "https://feeds.feedburner.com/silicon/feed/rss"
  "https://www.lemagit.fr/rss/Toutes-les-rubriques.xml"
  "https://www.journaldunet.com/rss/index.xml"
  "https://www.numerama.com/feed/"
  "https://www.clubic.com/feed/news.rss"
)

# ── Lecture des URLs ──
if [[ $# -gt 0 ]]; then
  if [[ "$1" == "-" ]]; then
    mapfile -t URLS < <(grep -vE '^[[:space:]]*(#|$)' /dev/stdin)
  else
    [[ -f "$1" ]] || { echo "ERR: fichier introuvable: $1" >&2; exit 2; }
    mapfile -t URLS < <(grep -vE '^[[:space:]]*(#|$)' "$1")
  fi
else
  URLS=("${DEFAULT_URLS[@]}")
fi

# Vérifie que xmllint est dispo
if ! command -v xmllint >/dev/null 2>&1; then
  echo "WARN: xmllint absent (apt-get install libxml2-utils) — détection items dégradée" >&2
  HAVE_XMLLINT=0
else
  HAVE_XMLLINT=1
fi

# Header TSV
printf "URL\tHTTP\tCONTENT_TYPE\tIS_XML\tITEMS\tLATEST_PUB\tVERDICT\n"

for url in "${URLS[@]}"; do
  # 1) Téléchargement
  tmp=$(mktemp)
  http_info=$(curl -sSL -A "$UA" -m "$TIMEOUT" \
    -o "$tmp" \
    -w "%{http_code}\t%{content_type}\n" \
    "$url" 2>/dev/null) || true

  if [[ -z "$http_info" ]]; then
    # curl a renvoyé un code d'erreur sans output
    err_code=$?
    case $err_code in
      6)  verdict="DNS_ERROR" ;;
      28) verdict="TIMEOUT" ;;
      *)  verdict="CURL_ERR_$err_code" ;;
    esac
    printf "%s\t-\t-\t-\t-\t-\t%s\n" "$url" "$verdict"
    rm -f "$tmp"
    continue
  fi

  http_code=$(echo "$http_info" | cut -f1)
  ctype=$(echo "$http_info" | cut -f2 | cut -d';' -f1 | tr -d ' ')

  # 2) HTTP non-2xx
  if [[ ! "$http_code" =~ ^2 ]]; then
    printf "%s\t%s\t%s\t-\t-\t-\tHTTP_%s\n" "$url" "$http_code" "$ctype" "$http_code"
    rm -f "$tmp"
    continue
  fi

  # 3) Détecte XML (heuristique : début de fichier <? ou <rss/<feed)
  first=$(head -c 200 "$tmp" | tr -d '[:space:]')
  if [[ ! "$first" =~ ^\<\? ]] && [[ ! "$first" =~ \<rss ]] && [[ ! "$first" =~ \<feed ]]; then
    printf "%s\t%s\t%s\tno\t-\t-\tNOT_XML\n" "$url" "$http_code" "$ctype"
    rm -f "$tmp"
    continue
  fi

  # 4) Parse items + latest pub
  items=0
  latest_pub="-"
  if [[ $HAVE_XMLLINT -eq 1 ]]; then
    # RSS 2.0 : //item ; Atom : //*[local-name()='entry']
    items=$(xmllint --xpath "count(//item) + count(//*[local-name()='entry'])" "$tmp" 2>/dev/null || echo 0)
    items=${items%.*}
    # pubDate (RSS) ou updated/published (Atom)
    pubs=$(xmllint --xpath "//pubDate/text() | //*[local-name()='updated']/text() | //*[local-name()='published']/text()" "$tmp" 2>/dev/null || true)
    if [[ -n "$pubs" ]]; then
      # parse chaque date, garde le max
      latest_epoch=0
      while IFS= read -r d; do
        d=$(echo "$d" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        [[ -z "$d" ]] && continue
        e=$(date -d "$d" +%s 2>/dev/null || echo 0)
        [[ "$e" -gt "$latest_epoch" ]] && latest_epoch=$e
      done <<< "$pubs"
      if [[ $latest_epoch -gt 0 ]]; then
        latest_pub=$(date -u -d "@$latest_epoch" +"%Y-%m-%d")
        age_days=$(( (NOW_EPOCH - latest_epoch) / 86400 ))
      fi
    fi
  fi

  # 5) Verdict
  if [[ $items -eq 0 ]]; then
    verdict="EMPTY"
  elif [[ "$latest_pub" == "-" ]]; then
    verdict="OK_NO_DATE"
  elif [[ ${age_days:-9999} -le 30 ]]; then
    verdict="OK_FRESH"
  else
    verdict="OK_STALE"
  fi

  printf "%s\t%s\t%s\tyes\t%s\t%s\t%s\n" "$url" "$http_code" "$ctype" "$items" "$latest_pub" "$verdict"
  rm -f "$tmp"
done
