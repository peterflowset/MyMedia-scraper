from __future__ import annotations

import logging
from datetime import datetime

import gspread

from models import Business

logger = logging.getLogger(__name__)

HEADERS = [
    "Firmenname",
    "Kategorie",
    "Adresse",
    "Stadt",
    "Telefon (Firma)",
    "Email (Firma)",
    "Website",
    "Google Rating",
    "Bewertungen",
    "Ansprechpartner 1 - Name",
    "Ansprechpartner 1 - Titel",
    "Ansprechpartner 1 - Email",
    "Ansprechpartner 1 - Telefon",
    "Ansprechpartner 2 - Name",
    "Ansprechpartner 2 - Titel",
    "Ansprechpartner 2 - Email",
    "Ansprechpartner 2 - Telefon",
]


class GoogleSheetsExporter:
    def __init__(
        self,
        service_account_file: str | None = None,
        service_account_info: dict | None = None,
    ):
        if service_account_info:
            self.gc = gspread.service_account_from_dict(service_account_info)
        else:
            self.gc = gspread.service_account(filename=service_account_file)

    def export(
        self, businesses: list[Business], business_type: str, city: str
    ) -> str:
        """Export businesses to a new Google Sheet. Returns the sheet URL."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        title = f"Leads_{business_type}_{city}_{date_str}"

        logger.info(f"Erstelle Google Sheet: '{title}'")
        spreadsheet = self.gc.create(title)
        spreadsheet.share('', perm_type='anyone', role='reader')

        worksheet = spreadsheet.sheet1
        worksheet.update_title("Leads")

        # Build all rows (header + data)
        rows = [HEADERS]
        for biz in businesses:
            rows.append(self._business_to_row(biz))

        # Batch write all data
        worksheet.update(
            rows,
            value_input_option="USER_ENTERED",
        )

        # Format header row (bold)
        worksheet.format("A1:Q1", {"textFormat": {"bold": True}})

        sheet_url = spreadsheet.url
        logger.info(f"Export abgeschlossen: {sheet_url}")
        return sheet_url

    def _business_to_row(self, biz: Business) -> list[str]:
        company_email = ", ".join(biz.company_emails) if biz.company_emails else ""
        company_phone = _strip_leading_plus(biz.phone)

        # Contact person 1
        c1_name = c1_title = c1_email = c1_phone = ""
        if len(biz.contact_persons) >= 1:
            c1 = biz.contact_persons[0]
            c1_name = c1.name
            c1_title = c1.title
            c1_email = c1.email
            c1_phone = _strip_leading_plus(c1.phone)

        # Contact person 2
        c2_name = c2_title = c2_email = c2_phone = ""
        if len(biz.contact_persons) >= 2:
            c2 = biz.contact_persons[1]
            c2_name = c2.name
            c2_title = c2.title
            c2_email = c2.email
            c2_phone = _strip_leading_plus(c2.phone)

        return [
            biz.name,
            biz.category,
            biz.address,
            biz.city,
            company_phone,
            company_email,
            biz.website,
            str(biz.google_rating) if biz.google_rating is not None else "",
            str(biz.review_count) if biz.review_count is not None else "",
            c1_name,
            c1_title,
            c1_email,
            c1_phone,
            c2_name,
            c2_title,
            c2_email,
            c2_phone,
        ]


def _strip_leading_plus(phone: str) -> str:
    text = phone.strip()
    if text.startswith("+"):
        return text[1:].lstrip()
    return text
