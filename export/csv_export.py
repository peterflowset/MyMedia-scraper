from __future__ import annotations

import csv
import io

from models import Business

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


def build_csv(businesses: list[Business]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(HEADERS)
    for biz in businesses:
        writer.writerow(_business_to_row(biz))
    return buf.getvalue()


def _business_to_row(biz: Business) -> list[str]:
    company_email = ", ".join(biz.company_emails) if biz.company_emails else ""
    company_phone = _strip_leading_plus(biz.phone)

    c1_name = c1_title = c1_email = c1_phone = ""
    if len(biz.contact_persons) >= 1:
        c1 = biz.contact_persons[0]
        c1_name = c1.name
        c1_title = c1.title
        c1_email = c1.email
        c1_phone = _strip_leading_plus(c1.phone)

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
