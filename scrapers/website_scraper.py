from __future__ import annotations

import json
import logging
import re
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

logger = logging.getLogger(__name__)

MAX_PAGES = 5
MAX_URLS = 200
CRAWL_DEPTH = 2
MIN_TEXT_LENGTH = 50

COMMON_CONTACT_PATHS = [
    "/team", "/kontakt", "/contact", "/contatti", "/impressum",
    "/about", "/about-us", "/chi-siamo", "/ueber-uns",
    "/mitarbeiter", "/staff", "/people", "/equipe",
    "/praxisteam", "/aerzte", "/doctors", "/azienda",
    "/unternehmen", "/company",
]

URL_FILTER_PROMPT = """\
Du bekommst eine Liste von URLs einer Firmen-Website. \
Wähle die URLs aus, die am wahrscheinlichsten Kontaktinformationen, \
Team-Mitglieder oder Ansprechpartner enthalten.

Relevante Seiten sind z.B.:
- Team / Über uns / Chi siamo / About
- Kontakt / Contatti / Contact
- Impressum
- Mitarbeiter / Staff / Ärzte / Doctors
- Praxisteam / Equipe

NICHT relevant sind:
- Produkte, Dienstleistungen, Behandlungen, Preise
- Blog-Posts, News, Artikel
- Datenschutz, AGB, Cookie-Richtlinien
- Bildergalerien, Downloads, Sitemaps (.xml)

Antworte NUR mit einem JSON-Array der relevanten URLs, z.B.:
["https://example.com/team", "https://example.com/kontakt"]

Wenn keine URL relevant ist, antworte mit: []
"""


class WebsiteScraper:
    def __init__(self, openrouter_api_key: str):
        self.llm = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_api_key,
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0 Safari/537.36"
                )
            }
        )
        self._playwright = None
        self._browser = None

    def close(self):
        """Close the persistent Playwright browser if open."""
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._playwright:
            try:
                self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    def __del__(self):
        self.close()

    def fetch_contact_pages(
        self, website_url: str, debug: bool = False, debug_slug: str | None = None
    ) -> tuple[dict[str, str], dict[str, list[str]]]:
        """Fetch relevant contact pages. Returns (pages_text, extracted_contact_data)."""
        if not website_url:
            return {}, {"emails": [], "phones": []}

        base_url = website_url.rstrip("/")
        if not base_url.startswith("http"):
            base_url = "https://" + base_url

        debug_dir = _debug_dir(debug_slug) if debug and debug_slug else None

        # Step 1: Discover URLs via crawl
        all_urls = self._discover_urls(base_url)
        if debug_dir is not None:
            _write_debug_text(debug_dir / "urls_all.txt", "\n".join(all_urls))

        # Step 2: LLM wählt relevante URLs aus
        urls_to_scrape = [base_url]
        if all_urls:
            relevant = self._llm_filter_urls(all_urls, base_url)
            if not relevant:
                relevant = _keyword_filter_urls(all_urls)
                if relevant:
                    logger.info(
                        f"Website {base_url}: Keyword-Fallback wählte {len(relevant)} URLs"
                    )
            if debug_dir is not None:
                _write_debug_json(debug_dir / "urls_selected.json", relevant)
            for url in relevant:
                if url not in urls_to_scrape:
                    urls_to_scrape.append(url)
            logger.info(
                f"Website {base_url}: LLM wählte {len(urls_to_scrape) - 1} "
                f"relevante Unterseiten aus {len(all_urls)} URLs"
            )

        urls_to_scrape = urls_to_scrape[:MAX_PAGES]

        # Step 3: Scrape each URL
        results: dict[str, str] = {}
        all_contact_data: dict[str, list[str]] = {"emails": [], "phones": []}
        for url in urls_to_scrape:
            text, contact_data = self._scrape_url(url)
            # Merge extracted contact data
            for email in contact_data.get("emails", []):
                if email not in all_contact_data["emails"]:
                    all_contact_data["emails"].append(email)
            for phone in contact_data.get("phones", []):
                if phone not in all_contact_data["phones"]:
                    all_contact_data["phones"].append(phone)

            if text and len(text) > MIN_TEXT_LENGTH:
                label = "homepage" if url == base_url else _url_to_label(url)
                results[label] = text
                if debug_dir is not None:
                    _write_debug_text(
                        debug_dir / "pages" / f"{_safe_label(label)}.txt", text
                    )

        logger.info(
            f"Website {base_url}: {len(results)} Seiten geladen "
            f"({', '.join(results.keys())}), "
            f"{len(all_contact_data['emails'])} Emails und "
            f"{len(all_contact_data['phones'])} Telefonnummern direkt extrahiert"
        )
        return results, all_contact_data

    def _discover_urls(self, base_url: str) -> list[str]:
        """Discover URLs using a tiered strategy: sitemap > common paths > BFS crawl."""
        # Tier 1: Try sitemap.xml
        sitemap_urls = self._parse_sitemap(base_url)
        if sitemap_urls:
            logger.info(f"Website {base_url}: {len(sitemap_urls)} URLs aus sitemap.xml")
            return sitemap_urls

        # Tier 2: Probe common contact paths
        common_urls = self._probe_common_paths(base_url)
        if common_urls:
            logger.info(
                f"Website {base_url}: {len(common_urls)} URLs über Common-Paths gefunden"
            )
            return common_urls

        # Tier 3: Fallback to BFS crawl
        logger.info(f"Website {base_url}: Fallback auf BFS-Crawl")
        return self._crawl_site(base_url, max_depth=CRAWL_DEPTH, limit=MAX_URLS)

    def _parse_sitemap(self, base_url: str) -> list[str]:
        """Try to parse /sitemap.xml and return all URLs."""
        sitemap_url = base_url.rstrip("/") + "/sitemap.xml"
        try:
            resp = self.session.get(sitemap_url, timeout=10)
            if resp.status_code >= 400:
                return []
            root = ET.fromstring(resp.content)
            # Handle namespace (most sitemaps use the sitemap protocol namespace)
            ns = ""
            if root.tag.startswith("{"):
                ns = root.tag.split("}")[0] + "}"
            urls = []
            for loc in root.iter(f"{ns}loc"):
                if loc.text:
                    url = loc.text.strip()
                    if not _is_asset(url):
                        urls.append(url)
            return urls[:MAX_URLS]
        except Exception as e:
            logger.debug(f"Sitemap-Parsing fehlgeschlagen für {base_url}: {e}")
            return []

    def _probe_common_paths(self, base_url: str) -> list[str]:
        """Probe common contact-related paths and return those that respond with 200."""
        found: list[str] = []
        for path in COMMON_CONTACT_PATHS:
            url = base_url.rstrip("/") + path
            try:
                resp = self.session.head(url, timeout=5, allow_redirects=True)
                if resp.status_code < 400:
                    found.append(url)
            except Exception:
                continue
        return found

    def _llm_filter_urls(self, urls: list[str], base_url: str) -> list[str]:
        url_list = "\n".join(urls)
        try:
            response = self.llm.chat.completions.create(
                model="google/gemini-2.5-flash",
                messages=[
                    {"role": "system", "content": URL_FILTER_PROMPT},
                    {
                        "role": "user",
                        "content": f"Website: {base_url}\n\nURLs:\n{url_list}",
                    },
                ],
                temperature=0.0,
                max_tokens=1000,
            )

            content = response.choices[0].message.content
            if not content:
                return []

            return self._parse_url_list(content)

        except Exception as e:
            logger.warning(f"LLM URL-Filter fehlgeschlagen: {e}")
            return []

    def _parse_url_list(self, response_text: str) -> list[str]:
        text = response_text.strip()
        # Handle markdown code blocks
        if "```" in text:
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("["):
                    text = part
                    break

        try:
            result = json.loads(text)
            if isinstance(result, list):
                return [u for u in result if isinstance(u, str)]
        except json.JSONDecodeError:
            logger.warning(f"Konnte LLM URL-Antwort nicht parsen: {text[:200]}")

        return []

    def _scrape_url(self, url: str) -> tuple[str | None, dict[str, list[str]]]:
        html = self._fetch_html(url)
        text = _html_to_text(html) if html else ""
        contact_data: dict[str, list[str]] = {"emails": [], "phones": []}

        if not text or len(text) < MIN_TEXT_LENGTH:
            html = self._render_with_playwright(url) or html
            text = _html_to_text(html) if html else ""
            if not text:
                return None, contact_data

        # Extract structured contact data from HTML before losing structure
        if html:
            contact_data = _extract_contact_data_from_html(html)

        # Clean out common noise before sending to LLM
        text = _clean_markdown(text)

        # Truncate very long pages
        if len(text) > 50_000:
            text = text[:50_000]

        return text, contact_data

    def _fetch_html(self, url: str) -> str | None:
        try:
            resp = self.session.get(url, timeout=20)
            if resp.status_code >= 400:
                return None
            return resp.text
        except Exception as e:
            logger.debug(f"HTTP fetch fehlgeschlagen für {url}: {e}")
            return None

    def _get_browser(self):
        """Lazily start a persistent Playwright browser instance."""
        if self._browser is None:
            try:
                from playwright.sync_api import sync_playwright
            except Exception:
                logger.debug("Playwright nicht installiert, JS-Render wird übersprungen")
                return None
            try:
                self._playwright = sync_playwright().start()
                self._browser = self._playwright.chromium.launch()
            except Exception as e:
                logger.debug(f"Playwright-Start fehlgeschlagen: {e}")
                self._playwright = None
                return None
        return self._browser

    def _render_with_playwright(self, url: str) -> str | None:
        browser = self._get_browser()
        if browser is None:
            return None
        try:
            page = browser.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()
            finally:
                page.close()
            return html
        except Exception as e:
            logger.debug(f"Playwright-Render fehlgeschlagen für {url}: {e}")
            return None

    def _crawl_site(self, base_url: str, max_depth: int, limit: int) -> list[str]:
        base_host = _host(base_url)
        seen: set[str] = set()
        queue: deque[tuple[str, int]] = deque([(base_url, 0)])
        results: list[str] = []

        while queue and len(results) < limit:
            url, depth = queue.popleft()
            url = _normalize_url(url)
            if not url or url in seen:
                continue
            if _is_asset(url):
                continue
            if base_host and _host(url) != base_host:
                continue

            seen.add(url)
            results.append(url)

            if depth >= max_depth:
                continue

            html = self._fetch_html(url)
            if not html:
                continue

            for link in _extract_links(html, url):
                if link not in seen:
                    queue.append((link, depth + 1))

        return results


# Patterns to strip from scraped markdown (cookie banners, nav menus, etc.)
_NOISE_PATTERNS = [
    # Cookie consent blocks
    re.compile(
        r"(?:Cookies? verwalten|Cookie[- ]?(?:Einstellungen|Settings|Richtlinie|Policy)).*?"
        r"(?:Einstellungen speichern|Akzeptieren|Accept|Ablehnen|Deny|Hide Toolbar)",
        re.DOTALL | re.IGNORECASE,
    ),
    # Accessibility toolbar
    re.compile(
        r"Accessibility Adjustments.*?Reset Settings",
        re.DOTALL | re.IGNORECASE,
    ),
    # Language selector blocks (long lists of flags/languages)
    re.compile(
        r"(?:English|Deutsch|Select your (?:language|accessibility)).*?"
        r"(?:Srpski|Українська|Hide Toolbar)",
        re.DOTALL | re.IGNORECASE,
    ),
    # Repeated navigation menus
    re.compile(r"(?:Gehe zu \.\.\.|Toggle Navigation)\n(?:.*?\n){2,20}", re.IGNORECASE),
    # Image-only lines (no useful text)
    re.compile(r"^!\[(?:flag|Symbol|toggle).*?\]\(.*?\)$", re.MULTILINE | re.IGNORECASE),
]


def _clean_markdown(text: str) -> str:
    """Remove cookie banners, navigation noise, and other boilerplate."""
    for pattern in _NOISE_PATTERNS:
        text = pattern.sub("", text)

    # Collapse excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _url_to_label(url: str) -> str:
    """Extract a readable label from a URL path."""
    path = url.rstrip("/").split("/")[-1]
    path = re.sub(r"[?#].*", "", path)
    return "/" + path if path else url


def _safe_label(label: str) -> str:
    name = label.strip().lstrip("/").replace("/", "_")
    name = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
    return name or "page"


def _extract_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        if not href:
            continue
        full = urljoin(base_url, href)
        full = _normalize_url(full)
        if full:
            links.append(full)
    return links


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def _extract_contact_data_from_html(html: str) -> dict[str, list[str]]:
    """Extract emails and phone numbers directly from mailto: and tel: links."""
    soup = BeautifulSoup(html, "html.parser")
    emails: list[str] = []
    phones: list[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if href.startswith("mailto:"):
            email = href[7:].split("?")[0].strip()
            if email and email not in emails:
                emails.append(email)
        elif href.startswith("tel:"):
            phone = href[4:].strip()
            if phone and phone not in phones:
                phones.append(phone)
    return {"emails": emails, "phones": phones}


def _keyword_filter_urls(urls: list[str]) -> list[str]:
    keywords = [
        "kontakt",
        "contact",
        "contatti",
        "impressum",
        "team",
        "about",
        "chi-siamo",
        "about-us",
        "people",
        "staff",
        "mitarbeiter",
        "praxis",
        "equipe",
        "azienda",
        "unternehmen",
    ]
    pattern = re.compile(r"(" + "|".join(re.escape(k) for k in keywords) + r")", re.I)
    filtered = [u for u in urls if pattern.search(u)]
    return filtered[:MAX_PAGES]


def _normalize_url(url: str) -> str | None:
    url = url.strip()
    if not url or url.startswith("mailto:") or url.startswith("tel:"):
        return None
    # Only strip fragment (anchor), keep query parameters for SPA routing
    url = re.sub(r"#.*$", "", url)
    return url


def _is_asset(url: str) -> bool:
    return bool(
        re.search(r"\.(?:pdf|jpg|jpeg|png|gif|svg|webp|zip|rar|7z|xml)$", url, re.I)
    )


def _host(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _debug_dir(slug: str) -> Path:
    path = Path("debug") / slug
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_debug_text(path: Path, text: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    except Exception as e:
        logger.debug(f"Debug-Datei konnte nicht geschrieben werden: {path} ({e})")


def _write_debug_json(path: Path, data) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.debug(f"Debug-JSON konnte nicht geschrieben werden: {path} ({e})")
