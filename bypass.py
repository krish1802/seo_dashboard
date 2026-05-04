#!/usr/bin/env python3
"""
Click-farm / search-bot tester — multi-site.

For every registered site, runs site:<domain> queries on Google / Yahoo / Bing
and counts how many result links it can open. Saves per-site daily totals to:
    seo_reports/<slug>/traffic_generated_YYYY-MM-DD.csv

CLI:
    python bypass.py                     # all sites, all engines
    python bypass.py --site sanfranciscobriefing.com
    python bypass.py --engines google.com,bing.com
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright
from seleniumbase import SB

from sites_config import Site, SITES, get_site


DEFAULT_ENGINES = ["google.com", "yahoo.com", "bing.com"]


# ── PER-ENGINE FLOW ─────────────────────────────────────────────────────

def run_for_engine(page, engine: str, domain: str) -> int:
    """Open `site:<domain>` on `engine` and click all matching results."""
    query = f"site:{domain}"
    clicks = 0

    if "google.com" in engine:
        page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=20000)
        try:
            page.locator(
                "button:has-text('I agree'), button:has-text('Accept all')"
            ).first.click(timeout=5000)
        except Exception:
            pass
        page.fill("textarea[name='q'], input[name='q']", query)
        page.keyboard.press("Enter")
        page.wait_for_selector("a h3", timeout=20000)
        link_locator = page.locator("a:has(h3)")

    elif "yahoo.com" in engine:
        page.goto("https://search.yahoo.com", wait_until="domcontentloaded", timeout=20000)
        page.fill("input[name='p']", query)
        page.keyboard.press("Enter")
        page.wait_for_selector("a.ac-algo, a[ref*='result']", timeout=20000)
        link_locator = page.locator("a.ac-algo, a[ref*='result']")

    elif "bing.com" in engine:
        page.goto("https://www.bing.com", wait_until="domcontentloaded", timeout=20000)
        page.fill("input[name='q']", query)
        page.keyboard.press("Enter")
        page.wait_for_selector("li.b_algo h2 a", timeout=20000)
        link_locator = page.locator("li.b_algo h2 a")

    else:
        return 0

    for i in range(link_locator.count()):
        href = link_locator.nth(i).get_attribute("href")
        if href and domain in href:
            clicks += 1
            new_page = page.context.new_page()
            try:
                new_page.goto(href, wait_until="domcontentloaded")
                new_page.wait_for_timeout(3000)
            except Exception:
                pass
            finally:
                new_page.close()
    return clicks


# ── PERSISTENCE ─────────────────────────────────────────────────────────

_NEW_HEADER = ["date", "run_timestamp", "site", "engine", "clicks"]


def save_daily_clicks(site: Site, results: dict, base_output: str = "seo_reports") -> Path:
    """Append this run's per-engine totals to the per-site daily CSV.

    Each cron run appends one row per engine, so the daily total is the SUM
    of every row for that date. The `run_timestamp` column lets you tell
    individual runs apart for auditing.

    Schema-safety: if the existing daily file uses the old 4-column schema
    (date,site,engine,clicks), we DO NOT append 5-column rows to it (that
    breaks pandas.read_csv). Instead, the old file is rewritten in-place
    with the new schema (run_timestamp = '' for legacy rows) before append.
    """
    now = datetime.utcnow()
    today = now.strftime("%Y-%m-%d")
    run_ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    out_dir = Path(site.output_dir(base_output))
    path = out_dir / f"traffic_generated_{today}.csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    _ensure_new_schema(path)

    file_exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(_NEW_HEADER)
        for engine, clicks in results.items():
            writer.writerow([today, run_ts, site.domain, engine, clicks])
        f.flush()
    return path


def _ensure_new_schema(path: Path) -> None:
    """Migrate a legacy 4-column CSV to the 5-column schema in place.

    No-op if the file doesn't exist, is empty, or already has the new header.
    """
    if not path.exists() or path.stat().st_size == 0:
        return
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        if not rows:
            return
        header = [c.strip().lower() for c in rows[0]]
        if header == _NEW_HEADER:
            return  # already migrated
        # Build a new file: header + each row padded to the new shape.
        new_rows = [_NEW_HEADER]
        for row in rows:
            stripped = [c.strip() for c in row]
            # Skip duplicate / legacy headers anywhere in the file.
            if [c.lower() for c in stripped] in (["date", "site", "engine", "clicks"], _NEW_HEADER):
                continue
            if len(stripped) == 4:
                # date, site, engine, clicks  ->  date, '', site, engine, clicks
                new_rows.append([stripped[0], "", stripped[1], stripped[2], stripped[3]])
            elif len(stripped) >= 5:
                new_rows.append(stripped[:5])
            # rows with <4 cols are dropped (malformed).
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(new_rows)
    except Exception as e:
        # If migration fails, rename the bad file aside so the next run starts fresh.
        backup = path.with_suffix(path.suffix + ".broken")
        try:
            path.rename(backup)
            print(f"⚠️  could not migrate {path.name} ({e}); moved to {backup.name}")
        except Exception:
            pass


# ── PER-SITE FLOW ───────────────────────────────────────────────────────

def run_site(site: Site, page, engines: list[str]) -> dict:
    daily = {e: 0 for e in engines}
    for engine in engines:
        try:
            print(f"  ▶ [{site.domain}] running {engine} ...")
            daily[engine] = run_for_engine(page, engine, site.domain)
            print(f"    {engine}: {daily[engine]} clicks")
        except Exception as exc:
            daily[engine] = 0
            print(f"    {engine}: error ({type(exc).__name__}: {exc}), recorded 0")
    out = save_daily_clicks(site, daily)
    print(f"  💾 [{site.domain}] saved → {out.resolve()}")
    return daily


# ── ENTRY ───────────────────────────────────────────────────────────────

def run_all(engines: list[str] = DEFAULT_ENGINES, only_site: str | None = None) -> dict[str, dict]:
    sites = [get_site(only_site)] if only_site else SITES
    overall: dict[str, dict] = {}

    with SB(uc=True) as sb:
        sb.activate_cdp_mode()
        endpoint_url = sb.cdp.get_endpoint_url()

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(endpoint_url)
            context = browser.contexts[0]
            page = context.pages[0]

            for site in sites:
                print(f"\n=== {site.domain} ===")
                try:
                    overall[site.domain] = run_site(site, page, engines)
                except Exception as exc:
                    print(f"❌ [{site.domain}] failed: {type(exc).__name__}: {exc}")
                    overall[site.domain] = {e_: 0 for e_ in engines}

            browser.close()

    return overall


def _main() -> None:
    ap = argparse.ArgumentParser(description="Multi-site search-engine click farm")
    ap.add_argument("--site", help="Run for one domain only")
    ap.add_argument("--engines", default=",".join(DEFAULT_ENGINES),
                    help="Comma-separated engines (default: google.com,yahoo.com,bing.com)")
    args = ap.parse_args()

    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    summary = run_all(engines=engines, only_site=args.site)

    print("\n──────── CLICK-FARM SUMMARY ────────")
    for domain, per in summary.items():
        total = sum(per.values())
        per_str = ", ".join(f"{e}={c}" for e, c in per.items())
        print(f"  {domain:35s}  total={total:3d}  ({per_str})")


if __name__ == "__main__":
    _main()
