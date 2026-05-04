"""
Central registry for all WordPress sites managed by the SEO toolkit.


Single source of truth. Every other script imports SITES from here.


Two ways to populate the registry:
  1. Hard-coded SITES list (default, below) — safe to commit (without secrets) or load from env.
  2. parse_wp_pass_file("wp_pass.txt") — parses the legacy plain-text creds file.


Secrets handling:
  - WP application passwords and the GA4 service-account JSON are sensitive.
  - In production, prefer environment variables (WP_APP_PASS_<DOMAIN_KEY>) or
    Streamlit secrets (st.secrets["sites"][domain]["wp_app_pass"]).
"""


from __future__ import annotations


import os
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional



# ──────────────────────────────────────────────────────────────────────────
# Site model
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class Site:
    """One managed WordPress site."""
    domain: str                           # e.g. "sanfranciscobriefing.com"
    site_url: str                         # e.g. "https://sanfranciscobriefing.com"
    brand_name: str                       # human-readable name shown in UI
    wp_user: str                          # WordPress username (or email)
    wp_app_pass: str                      # WP application password (with spaces)
    ga4_property_id: str = ""             # GA4 property ID (string)
    site_description: str = ""            # short description
    brand_logo_url: str = ""              # absolute URL to logo image
    tracked_keywords: list[str] = field(default_factory=list)
    competitors: list[str] = field(default_factory=list)


    @property
    def api_base(self) -> str:
        return f"{self.site_url.rstrip('/')}/wp-json/wp/v2"


    @property
    def wp_url(self) -> str:
        return self.site_url.rstrip("/")


    @property
    def slug(self) -> str:
        """Filesystem-safe key derived from domain — used for output dirs."""
        return re.sub(r"[^a-z0-9]+", "_", self.domain.lower()).strip("_")


    @property
    def env_key(self) -> str:
        """Env-var suffix, e.g. SANFRANCISCOBRIEFING_COM."""
        return re.sub(r"[^A-Z0-9]+", "_", self.domain.upper()).strip("_")


    def output_dir(self, base: str = "seo_reports") -> str:
        """Per-site report directory: seo_reports/<slug>/"""
        path = os.path.join(base, self.slug)
        os.makedirs(path, exist_ok=True)
        return path


    def as_dict(self) -> dict:
        return asdict(self)



# ──────────────────────────────────────────────────────────────────────────
# Registry — all 16 managed sites
# ──────────────────────────────────────────────────────────────────────────
#
# Default brand names are inferred from the domain. Override here as needed.
# Per-site WP_APP_PASS may also be sourced from env: WP_APP_PASS_<ENV_KEY>.
# Falls back to the literal value baked in below.
#


_DEFAULT_KEYWORDS = {
    "sanfranciscobriefing.com": [
        "San Francisco news", "SF local news", "Bay Area news",
        "San Francisco politics", "San Francisco business news",
        "San Francisco events", "San Francisco neighborhood news",
    ],
    "irvineweeklydigest.com": [
        "Irvine news", "Irvine local news", "Orange County news",
        "Irvine business", "Irvine events", "Irvine real estate",
    ],
    "b2bmoversdaily.com": [
        "B2B moving industry", "commercial moving news", "logistics news",
        "moving company industry", "B2B logistics",
    ],
    "themiamientrepreneur.com": [
        "Miami entrepreneurs", "Miami startups", "Miami business news",
        "South Florida startups", "Miami tech",
    ],
    "losangelesinfluence.com": [
        "Los Angeles influencers", "LA culture", "LA lifestyle",
        "Los Angeles entertainment", "LA business",
    ],
    "nyartisinal.com": [
        "NY artisans", "New York craft", "artisanal New York",
        "NYC makers", "small batch New York",
    ],
    "lasvegasmonthlyreview.com": [
        "Las Vegas news", "Vegas business", "Las Vegas events",
        "Vegas entertainment", "Nevada news",
    ],
    "newyorkdailydigest.com": [
        "New York news", "NYC news", "New York City news",
        "Manhattan news", "NYC business",
    ],
    "newyorkluxurymag.com": [
        "New York luxury", "NYC luxury lifestyle", "NY high end",
        "Manhattan luxury", "luxury New York real estate",
    ],
    "culturdceo.com": [
        "CEO culture", "leadership news", "executive insights",
        "corporate culture", "C-suite news",
    ],
    "thenewyorkentrepreneur.com": [
        "New York entrepreneurs", "NYC startups", "NY business news",
        "Manhattan startups", "NY founders",
    ],
    "thelosangelesentrepreneur.com": [
        "Los Angeles entrepreneurs", "LA startups", "LA business news",
        "Southern California startups", "LA founders",
    ],
    "imperiumlivetv.com": [
        "Imperium Live", "live streaming news", "live TV coverage",
        "breaking news live", "live entertainment streaming",
    ],
    "manhattanyearly.com": [
        "Manhattan annual review", "Manhattan year in review", "Manhattan culture",
        "Manhattan lifestyle", "NYC annual feature",
    ],
    "londoninfluencerdaily.com": [
        "London influencers", "UK influencer news", "London social media",
        "London lifestyle", "London creator economy",
    ],
    "miamiheralddaily.com": [
        "Miami daily news", "Miami headlines", "South Florida news",
        "Miami breaking news", "Miami events",
    ],
}



def _brand_from_domain(domain: str) -> str:
    """sanfranciscobriefing.com -> 'San Francisco Briefing' (best-effort)."""
    name = domain.split(".")[0]
    # Hand-tuned mappings for nicer display names
    overrides = {
        "sanfranciscobriefing": "San Francisco Briefing",
        "irvineweeklydigest": "Irvine Weekly Digest",
        "b2bmoversdaily": "B2B Movers Daily",
        "themiamientrepreneur": "The Miami Entrepreneur",
        "losangelesinfluence": "Los Angeles Influence",
        "nyartisinal": "NY Artisanal",
        "lasvegasmonthlyreview": "Las Vegas Monthly Review",
        "newyorkdailydigest": "New York Daily Digest",
        "newyorkluxurymag": "New York Luxury Mag",
        "culturdceo": "Cultured CEO",
        "thenewyorkentrepreneur": "The New York Entrepreneur",
        "thelosangelesentrepreneur": "The Los Angeles Entrepreneur",
        "imperiumlivetv": "Imperium Live TV",
        "manhattanyearly": "Manhattan Yearly",
        "londoninfluencerdaily": "London Influencer Daily",
        "miamiheralddaily": "Miami Herald Daily",
    }
    if name in overrides:
        return overrides[name]
    # Fallback: title-case the domain root
    return name.replace("-", " ").title()



def _env_or(default: str, env_key: str) -> str:
    """Prefer env var if present and non-empty, else default."""
    val = os.getenv(env_key, "").strip()
    return val or default



# Raw cred rows — domain, user, app-pass, GA4 property
# (kept here so the file is self-contained; rotate/move to env or secrets in prod)
_RAW_SITES: list[tuple[str, str, str, str]] = [
    ("sanfranciscobriefing.com",     "testing",                  "sTz9 HbAF ROBO prvo SrI2 gJb7", "534913592"),
    ("irvineweeklydigest.com",       "pangeaiimp@gmail.com",     "Jv8Y PPHa H4Lo 8ulW CbEC 43wb", "535749103"),
    ("b2bmoversdaily.com",           "editorialstaff",           "qqN3 3iDt qGBT ZyG2 k2Fl jjbo", "535799209"),
    ("themiamientrepreneur.com",     "texasfashioninsider",      "thoE HfBm yc0X PssD qLmF xhkP", "535799207"),
    ("losangelesinfluence.com",      "losangelesinfluence",      "lu8C nbFh rIXi B85M zxdp MSgB", "535799884"),
    ("nyartisinal.com",              "texasfashioninsider",      "316l SeLI M4Nf cKte Xrdh h9PS", "535796573"),
    ("lasvegasmonthlyreview.com",    "texasfashioninsider",      "JvhM jN72 Nhv6 uGHu Gr1k Ik2v", "535810926"),
    ("newyorkdailydigest.com",       "editorialstaff",           "m53D iAUl wkXA QlsN KEOt jtRA", "535730287"),
    ("newyorkluxurymag.com",         "pangeaiimp@gmail.com",     "Om18 yi1k KDfd pBxp lpJU onJA", "535729084"),
    ("culturdceo.com",               "autofuturesweekly",        "C248 yEXG 4D2L DxQ4 kAeI xVA2", "535730288"),
    ("thenewyorkentrepreneur.com",   "pangeaiimp@gmail.com",     "LIJZ xVLM Eo3Q KIXK swCF ZsFf", "535730849"),
    ("thelosangelesentrepreneur.com","texasfashioninsider",      "dunn KBPl 48AU FtEJ vxqC auuy", "535745293"),
    ("imperiumlivetv.com",           "root",                     "1TAr yyyb gRqa udcy zgNk O7sv", "536000791"),
    ("manhattanyearly.com",          "texasfashioninsider",      "WeIH VJ3z fAO3 o1P6 hx0F dKtY", "536006118"),
    ("londoninfluencerdaily.com",    "editorialstaff",           "qzAD HzQT OZ0Q wjJP ZqG0 7ZpT", "536002535"),
    ("miamiheralddaily.com",         "editorialstaff",           "eorH aJ8k SpWV vAvj Sym6 Kb69", "535997361"),
]



def _build_default_sites() -> list[Site]:
    sites: list[Site] = []
    for domain, user, app_pass, ga4_id in _RAW_SITES:
        site_url = f"https://{domain}"
        env_key = re.sub(r"[^A-Z0-9]+", "_", domain.upper()).strip("_")
        sites.append(Site(
            domain=domain,
            site_url=site_url,
            brand_name=_brand_from_domain(domain),
            wp_user=_env_or(user, f"WP_USER_{env_key}"),
            wp_app_pass=_env_or(app_pass, f"WP_APP_PASS_{env_key}"),
            ga4_property_id=_env_or(ga4_id, f"GA4_PROPERTY_ID_{env_key}"),
            brand_logo_url=os.getenv(f"BRAND_LOGO_URL_{env_key}", f"{site_url}/wp-content/uploads/logo.png"),
            site_description=f"News, business and culture coverage from {_brand_from_domain(domain)}.",
            tracked_keywords=_DEFAULT_KEYWORDS.get(domain, []),
            competitors=[],
        ))
    return sites



SITES: list[Site] = _build_default_sites()
SITES_BY_DOMAIN: dict[str, Site] = {s.domain: s for s in SITES}



# ──────────────────────────────────────────────────────────────────────────
# Lookup helpers
# ──────────────────────────────────────────────────────────────────────────


def get_site(domain_or_url: str) -> Site:
    """Look up a site by domain (or full URL). Raises KeyError if unknown."""
    key = domain_or_url.strip().lower()
    key = re.sub(r"^https?://", "", key).rstrip("/")
    if key.startswith("www."):
        key = key[4:]
    # Allow path component to come along — strip it
    key = key.split("/", 1)[0]
    if key not in SITES_BY_DOMAIN:
        raise KeyError(f"Unknown site: {domain_or_url!r}. Known: {list(SITES_BY_DOMAIN)}")
    return SITES_BY_DOMAIN[key]



def list_domains() -> list[str]:
    return [s.domain for s in SITES]



# ──────────────────────────────────────────────────────────────────────────
# Optional: parse legacy wp_pass.txt
# ──────────────────────────────────────────────────────────────────────────


_SITE_RE     = re.compile(r"^\s*SITE\s*:\s*(.+?)\s*$", re.I)
_USER_RE     = re.compile(r"^\s*WP_(?:USER(?:NAME)?)\s*:\s*(.+?)\s*$", re.I)
_PASS_RE     = re.compile(r"^\s*WP_PASS\s*:\s*(.+?)\s*$", re.I)
_GA4_RE      = re.compile(r"^\s*GA4_PROPERTY_ID\s*=?\s*\"?([^\"\s]+)\"?\s*$", re.I)



def parse_wp_pass_file(path: str | Path) -> list[Site]:
    """Parse the legacy plain-text wp_pass.txt format into Site objects."""
    path = Path(path)
    rows: list[dict] = []
    cur: dict = {}


    def flush():
        if cur.get("domain"):
            rows.append(cur.copy())
        cur.clear()


    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            # Blank line = block separator
            if cur.get("domain"):
                flush()
            continue
        m = _SITE_RE.match(line)
        if m:
            if cur.get("domain"):
                flush()
            domain = m.group(1).strip()
            domain = re.sub(r"^https?://", "", domain).rstrip("/").lower()
            if domain.startswith("www."):
                domain = domain[4:]
            cur["domain"] = domain
            continue
        m = _USER_RE.match(line)
        if m:
            cur["wp_user"] = m.group(1).strip()
            continue
        m = _PASS_RE.match(line)
        if m:
            cur["wp_app_pass"] = m.group(1).strip()
            continue
        m = _GA4_RE.match(line)
        if m:
            cur["ga4_property_id"] = m.group(1).strip()
            continue
    flush()


    sites: list[Site] = []
    for r in rows:
        domain = r["domain"]
        sites.append(Site(
            domain=domain,
            site_url=f"https://{domain}",
            brand_name=_brand_from_domain(domain),
            wp_user=r.get("wp_user", ""),
            wp_app_pass=r.get("wp_app_pass", ""),
            ga4_property_id=r.get("ga4_property_id", ""),
            brand_logo_url=f"https://{domain}/wp-content/uploads/logo.png",
            site_description=f"News, business and culture coverage from {_brand_from_domain(domain)}.",
            tracked_keywords=_DEFAULT_KEYWORDS.get(domain, []),
            competitors=[],
        ))
    return sites



def reload_from_wp_pass(path: str | Path = "wp_pass.txt") -> list[Site]:
    """Replace the in-memory registry with sites parsed from wp_pass.txt."""
    global SITES, SITES_BY_DOMAIN
    parsed = parse_wp_pass_file(path)
    if parsed:
        SITES = parsed
        SITES_BY_DOMAIN = {s.domain: s for s in SITES}
    return SITES



# ──────────────────────────────────────────────────────────────────────────
# Active site (used by Streamlit dashboard for module-level mutation)
# ──────────────────────────────────────────────────────────────────────────


_ACTIVE_DOMAIN: Optional[str] = None



def set_active(domain: str) -> Site:
    """Mark a site as active in the process. Returns the resolved Site."""
    global _ACTIVE_DOMAIN
    site = get_site(domain)
    _ACTIVE_DOMAIN = site.domain
    return site



def get_active() -> Site:
    """Return active site, defaulting to the first registered site."""
    if _ACTIVE_DOMAIN and _ACTIVE_DOMAIN in SITES_BY_DOMAIN:
        return SITES_BY_DOMAIN[_ACTIVE_DOMAIN]
    return SITES[0]



if __name__ == "__main__":
    print(f"Loaded {len(SITES)} sites:")
    for s in SITES:
        print(f"  {s.domain:35s}  user={s.wp_user:25s}  ga4={s.ga4_property_id}")
