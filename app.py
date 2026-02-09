import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st

from cache import (
    get_cached_businesses,
    get_cached_enrichment,
    set_cached_businesses,
    set_cached_enrichment,
)
from config import Config
from enrichment.contact_enricher import ContactEnricher
from export.google_sheets import GoogleSheetsExporter
from scrapers.outscraper_client import OutscraperService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="MyMedia Lead Scraper", page_icon="📋", layout="centered")
st.title("MyMedia Lead Scraper")
st.markdown("Automatisierte Lead-Generierung via Google Maps + Website-Scraping")

# --- Config check ---
try:
    config = Config.from_env()
except ValueError as e:
    st.error(f"Konfigurationsfehler: {e}")
    st.info("Bitte `.env` Datei mit den erforderlichen API-Keys anlegen (siehe `.env.example`).")
    st.stop()

# --- Input Form ---
with st.form("scrape_form"):
    col1, col2 = st.columns(2)
    with col1:
        country = st.text_input("Land (ISO-Code)", value="IT", help="z.B. IT, DE, AT")
        business_type = st.text_input("Branche", value="Zahnarzt", help="z.B. Zahnarzt, Restaurant, Anwalt")
    with col2:
        city = st.text_input("Stadt", value="Bozen", help="z.B. Bozen, München, Wien")
        num_leads = st.number_input("Anzahl Leads", min_value=1, max_value=100, value=20)
    debug_contacts = st.checkbox(
        "Debug: Kontakt-Scrape protokollieren",
        value=False,
        help="Speichert URLs, Seiteninhalte und LLM-Antworten pro Firma in ./debug/",
    )

    submitted = st.form_submit_button("Scrapen starten", type="primary", use_container_width=True)

if not submitted:
    st.stop()

# --- Pipeline ---
progress = st.progress(0, text="Starte...")
status = st.empty()

# Stage 1: OutScraper (with cache)
status.info("🔍 Suche Firmen auf Google Maps...")
progress.progress(5, text="Google Maps Suche...")

businesses = get_cached_businesses(business_type, city, country, num_leads)
if businesses:
    logger.info(f"OutScraper-Ergebnisse aus Cache geladen ({len(businesses)} Firmen)")
    progress.progress(30, text=f"{len(businesses)} Firmen aus Cache geladen")
    status.success(f"✅ {len(businesses)} Firmen aus Cache geladen")
else:
    try:
        outscraper = OutscraperService(config.outscraper_api_key)
        businesses = outscraper.search_businesses(
            business_type=business_type,
            city=city,
            country=country,
            limit=num_leads,
        )
    except Exception as e:
        st.error(f"OutScraper-Fehler: {e}")
        logger.exception("OutScraper-Fehler")
        st.stop()

    if not businesses:
        st.warning("Keine Firmen gefunden. Bitte Suchbegriffe anpassen.")
        st.stop()

    set_cached_businesses(business_type, city, country, num_leads, businesses)
    progress.progress(30, text=f"{len(businesses)} Firmen gefunden")
    status.success(f"✅ {len(businesses)} Firmen von Google Maps geladen")

# Stage 2: Contact Enrichment (parallel)
status_enrich = st.empty()
status_enrich.info("👤 Suche Ansprechpartner auf Firmen-Websites...")

MAX_WORKERS = 4
enrichers = [ContactEnricher(openrouter_api_key=config.openrouter_api_key) for _ in range(MAX_WORKERS)]


def _enrich_worker(args: tuple[int, "Business"]) -> tuple[int, "Business"]:
    idx, biz = args
    # Check enrichment cache first
    cached = get_cached_enrichment(biz.name, biz.website)
    if cached is not None:
        logger.info(f"'{biz.name}': Enrichment aus Cache geladen")
        return idx, cached
    enricher = enrichers[idx % MAX_WORKERS]
    try:
        result = enricher.enrich_business(biz, debug=debug_contacts)
        set_cached_enrichment(result)
        return idx, result
    except Exception as e:
        logger.error(f"Enrichment-Fehler für '{biz.name}': {e}")
        return idx, biz


enriched = [None] * len(businesses)
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = {
        pool.submit(_enrich_worker, (i, biz)): i
        for i, biz in enumerate(businesses)
    }
    done_count = 0
    for future in as_completed(futures):
        idx, result_biz = future.result()
        enriched[idx] = result_biz
        done_count += 1
        pct = 30 + int((done_count / len(businesses)) * 50)
        progress.progress(pct, text=f"Enrichment: {done_count}/{len(businesses)} fertig")

for e in enrichers:
    e.close()

businesses = enriched
total_contacts = sum(len(b.contact_persons) for b in businesses)
progress.progress(80, text="Enrichment abgeschlossen")
status_enrich.success(f"✅ {total_contacts} Ansprechpartner gefunden")

# Stage 3: Google Sheets Export
status_export = st.empty()
status_export.info("📊 Exportiere nach Google Sheets...")
progress.progress(85, text="Google Sheets Export...")

try:
    if "gcp_service_account" in st.secrets:
        exporter = GoogleSheetsExporter(service_account_info=dict(st.secrets["gcp_service_account"]))
    else:
        exporter = GoogleSheetsExporter(service_account_file=config.google_service_account_file)
    sheet_url = exporter.export(businesses, business_type, city)
except Exception as e:
    st.error(f"Google Sheets Export-Fehler: {e}")
    logger.exception("Export-Fehler")
    st.stop()

progress.progress(100, text="Fertig!")
status_export.success("✅ Export abgeschlossen")

# --- Result ---
st.divider()
st.subheader("Ergebnis")
st.metric("Firmen", len(businesses))
st.metric("Ansprechpartner", total_contacts)
st.markdown(f"**[Google Sheet öffnen]({sheet_url})**")
st.balloons()
