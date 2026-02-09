from __future__ import annotations

import logging

from outscraper import ApiClient
from tenacity import retry, stop_after_attempt, wait_exponential

from models import Business

logger = logging.getLogger(__name__)


class OutscraperService:
    def __init__(self, api_key: str):
        self.client = ApiClient(api_key=api_key)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
    def _api_search(self, query: str, limit: int, region: str) -> list[dict]:
        results = self.client.google_maps_search(
            query,
            limit=limit,
            region=region,
            language="de",
            enrichment=["domains_service"],
        )
        if not results or not results[0]:
            return []
        return results[0]

    def search_businesses(
        self, business_type: str, city: str, country: str, limit: int = 20
    ) -> list[Business]:
        query = f"{business_type} in {city}"
        logger.info(f"OutScraper-Suche: '{query}', region={country}, limit={limit}")

        raw_results = self._api_search(query, limit, country)
        if not raw_results:
            logger.warning("Keine Ergebnisse von OutScraper erhalten")
            return []

        businesses = []
        for item in raw_results:
            try:
                business = self._parse_result(item)
                if business:
                    businesses.append(business)
            except Exception as e:
                name = item.get("name", "???")
                logger.warning(f"Fehler beim Parsen von '{name}': {e}")

        logger.info(f"{len(businesses)} Firmen gefunden")
        return businesses

    def _parse_result(self, item: dict) -> Business | None:
        if not item.get("name"):
            return None

        # Collect emails from domains_service enrichment (email_1, email_2, email_3)
        emails = []
        for i in range(1, 4):
            email = item.get(f"email_{i}")
            if email and email not in emails:
                emails.append(email)

        return Business(
            name=item.get("name") or "",
            category=item.get("category") or item.get("type") or "",
            address=item.get("full_address") or item.get("address") or "",
            city=item.get("city") or "",
            phone=item.get("phone") or "",
            website=item.get("website") or "",
            company_emails=emails,
            google_rating=item.get("rating"),
            review_count=item.get("reviews"),
        )
