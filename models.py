from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ContactPerson(BaseModel):
    name: str = ""
    title: str = ""
    email: str = ""
    email_source: str = ""  # "website", "impressum", etc.
    phone: str = ""


class Business(BaseModel):
    name: str = ""
    category: str = ""
    address: str = ""
    city: str = ""
    phone: str = ""
    website: str = ""
    company_emails: list[str] = Field(default_factory=list)
    google_rating: Optional[float] = None
    review_count: Optional[int] = None
    contact_persons: list[ContactPerson] = Field(default_factory=list)


class ScrapeJob(BaseModel):
    country: str
    business_type: str
    city: str
    num_leads: int
    businesses: list[Business] = Field(default_factory=list)
