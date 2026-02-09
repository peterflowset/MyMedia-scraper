from __future__ import annotations

import json
import logging
import re
import time

from openai import OpenAI

from models import Business, ContactPerson
from scrapers.website_scraper import WebsiteScraper

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
Du bist ein Daten-Extraktions-Assistent. Deine Aufgabe ist es, Kontaktpersonen \
aus Website-Texten zu extrahieren.

Extrahiere maximal 2 Ansprechpartner mit folgenden Feldern:
- name: Vollständiger Name der Person
- title: Titel, Rolle oder Position (z.B. "Dr.", "Zahnarzt", "Geschäftsführer")
- email: Persönliche E-Mail-Adresse (NICHT allgemeine info@/office@ Adressen)
- phone: Persönliche Durchwahl oder Telefonnummer

Wichtige Regeln:
- Nur ECHTE Personen extrahieren, keine Firmennamen
- Allgemeine E-Mails wie info@, office@, kontakt@, praxis@ sind KEINE persönlichen E-Mails
- Wenn keine persönliche E-Mail vorhanden ist, darf eine allgemeine E-Mail genutzt werden
- Bevorzuge Personen mit Leitungsfunktion oder Inhaber

Antworte NUR mit validem JSON in diesem Format:
{
  "contacts": [
    {
      "name": "Dr. Max Mustermann",
      "title": "Zahnarzt / Praxisinhaber",
      "email": "max.mustermann@praxis.de",
      "phone": "+39 0471 123456"
    }
  ]
}

Wenn du keine Kontaktpersonen findest, antworte mit:
{"contacts": []}
"""

MAX_TEXT_PER_REQUEST = 15_000


class ContactEnricher:
    def __init__(self, openrouter_api_key: str):
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_api_key,
        )
        self.scraper = WebsiteScraper(
            openrouter_api_key=openrouter_api_key,
        )

    def close(self):
        """Close underlying scraper resources (e.g. Playwright browser)."""
        self.scraper.close()

    def enrich_business(self, business: Business, debug: bool = False) -> Business:
        """Enrich a business with contact persons from its website."""
        if not business.website:
            logger.info(f"'{business.name}': Keine Website, überspringe Enrichment")
            return business

        # Fetch website pages
        pages, contact_data = self.scraper.fetch_contact_pages(
            business.website, debug=debug, debug_slug=_slugify(business.name)
        )
        if not pages:
            logger.info(f"'{business.name}': Keine Seiteninhalte gefunden")
            return business

        # Combine page texts for LLM
        combined_text = self._combine_pages(pages)

        # Extract contacts via LLM (pass structured contact data for better results)
        contacts, raw_response = self._extract_contacts(
            combined_text, business.name, contact_data
        )
        if debug:
            _write_debug_text(
                _debug_dir(_slugify(business.name)) / "llm_response.json",
                raw_response or "",
            )
        if contacts:
            business.contact_persons = contacts[:2]
            logger.info(
                f"'{business.name}': {len(business.contact_persons)} Kontakt(e) gefunden"
            )
        else:
            logger.info(f"'{business.name}': Keine Kontaktpersonen gefunden")

        # Rate limiting
        time.sleep(1.5)

        return business

    def _combine_pages(self, pages: dict[str, str]) -> str:
        parts = []
        total_len = 0
        for page_name, text in pages.items():
            remaining = MAX_TEXT_PER_REQUEST - total_len
            if remaining <= 0:
                break
            chunk = text[:remaining]
            parts.append(f"--- Seite: {page_name} ---\n{chunk}")
            total_len += len(chunk)
        return "\n\n".join(parts)

    def _extract_contacts(
        self,
        website_text: str,
        business_name: str,
        contact_data: dict[str, list[str]] | None = None,
    ) -> tuple[list[ContactPerson], str | None]:
        # Build hint about structured data extracted from HTML
        hints = ""
        if contact_data:
            if contact_data.get("emails"):
                hints += f"\nDirekt aus HTML extrahierte E-Mail-Adressen: {', '.join(contact_data['emails'])}"
            if contact_data.get("phones"):
                hints += f"\nDirekt aus HTML extrahierte Telefonnummern: {', '.join(contact_data['phones'])}"

        user_prompt = (
            f"Firma: {business_name}\n\n"
            f"Website-Inhalte:\n{website_text}\n"
            f"{hints}\n\n"
            "Extrahiere die Kontaktpersonen als JSON."
        )

        try:
            response = self.client.chat.completions.create(
                model="google/gemini-2.5-flash",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=1000,
            )

            content = response.choices[0].message.content
            if not content:
                return [], None

            return self._parse_llm_response(content), content

        except Exception as e:
            logger.error(f"LLM-Fehler für '{business_name}': {e}")
            return [], None

    def _parse_llm_response(self, response_text: str) -> list[ContactPerson]:
        # Try to extract JSON from response (handle markdown code blocks)
        text = response_text.strip()
        if "```" in text:
            # Extract content between code fences
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    text = part
                    break

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning(f"Konnte LLM-Antwort nicht parsen: {text[:200]}")
            return []

        contacts = []
        for c in data.get("contacts", []):
            if not c.get("name"):
                continue
            email = c.get("email", "")
            if email and not _is_valid_email(email):
                logger.warning(f"Ungültige Email vom LLM verworfen: {email}")
                email = ""
            contacts.append(
                ContactPerson(
                    name=c.get("name", ""),
                    title=c.get("title", ""),
                    email=email,
                    email_source="website" if email else "",
                    phone=c.get("phone", ""),
                )
            )
        return contacts


_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def _is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email.strip()))


def _slugify(text: str) -> str:
    import re

    slug = text.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug or "business"


def _debug_dir(slug: str):
    from pathlib import Path

    path = Path("debug") / slug
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_debug_text(path, text: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    except Exception as e:
        logger.debug(f"Debug-Datei konnte nicht geschrieben werden: {path} ({e})")
