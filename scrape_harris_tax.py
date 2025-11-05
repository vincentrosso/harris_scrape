"""
Playwright scraping script for:
  1) https://harris.trueprodigy-taxtransparency.com/taxTransparency/propertySearch
  2) https://www.hctax.net/Property/pdf?t=print&Account=0552850000031

Outputs:
  - harris_trueprodigy_0552850000031.json  (parsed page data, if found)
  - hctax_0552850000031.pdf               (downloaded PDF from hctax.net)
"""

import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

ACCOUNT = "0552850000031"
OUT_DIR = Path(".")
OUT_DIR.mkdir(exist_ok=True)

def try_fill_selectors(page, selectors, value, timeout=3000):
    """
    Try multiple selectors until one works. Returns True if filled.
    """
    for sel in selectors:
        try:
            if page.query_selector(sel):
                page.fill(sel, value, timeout=timeout)
                return True
        except PlaywrightTimeout:
            continue
        except Exception:
            # some selectors may raise if not present, skip
            continue
    return False

def try_click_selectors(page, selectors, timeout=5000):
    for sel in selectors:
        try:
            if page.query_selector(sel):
                page.click(sel, timeout=timeout)
                return True
        except PlaywrightTimeout:
            continue
        except Exception:
            continue
    return False

def extract_table_like(page):
    """
    Generic table extraction: looks for <table> elements and returns
    row/column text. If no table exists, grabs main content text.
    """
    tables = page.query_selector_all("table")
    results = []
    if tables:
        for t in tables:
            headers = [h.inner_text().strip() for h in t.query_selector_all("thead th")] if t.query_selector("thead") else []
            rows = []
            for r in t.query_selector_all("tbody tr") or t.query_selector_all("tr"):
                cells = [c.inner_text().strip() for c in r.query_selector_all("th, td")]
                if cells:
                    rows.append(cells)
            results.append({"headers": headers, "rows": rows})
        return {"tables": results}
    # fallback: grab some common fields by label / text
    main_text = page.inner_text("body")[:10000]
    return {"text_snippet": main_text}

def scrape_trueprodigy(page):
    """
    Attempt to search by Account # and extract result.
    This site uses a search form — selectors may change; we try several options.
    """
    # heuristics: try common input names/ids
    possible_inputs = [
        "input[name='account']", "input[name='Account']", "input#Account",
        "input[name='parcel']", "input[name='searchText']", "input[type='text']",
        "input[placeholder*='Account']", "input[placeholder*='Search']"
    ]
    filled = try_fill_selectors(page, possible_inputs, ACCOUNT)
    if not filled:
        # try to fill any visible text inputs (first match)
        try:
            text_inputs = page.query_selector_all("input[type='text'], input:not([type])")
            if text_inputs:
                # fill first visible one
                for ti in text_inputs:
                    try:
                        if ti.is_visible():
                            ti.fill(ACCOUNT)
                            filled = True
                            break
                    except Exception:
                        continue
        except Exception:
            filled = False

    # try clicking a search button
    clicked = try_click_selectors(page, [
        "button[type='submit']",
        "button:has-text('Search')",
        "button:has-text('Go')",
        "input[type='submit']",
        "a:has-text('Search')"
    ])
    # if no explicit click, try pressing Enter in the focused input
    if not clicked and filled:
        page.keyboard.press("Enter")

    # wait for navigation / results to load
    try:
        page.wait_for_load_state("networkidle", timeout=8000)
    except PlaywrightTimeout:
        # continue anyway, page may update via XHR
        pass

    # give JS a moment to render results
    time.sleep(1.0)

    # look for results; often results may be in a div or table
    # if the account number appears on the page, grab table/text
    try:
        body_text = page.inner_text("body")
        if ACCOUNT in body_text:
            return extract_table_like(page)
        else:
            # try to find links that include account or 'Property' to follow
            link = page.query_selector(f"a[href*='{ACCOUNT}']")
            if link:
                link.click()
                try:
                    page.wait_for_load_state("networkidle", timeout=6000)
                except Exception:
                    pass
                time.sleep(0.5)
                return extract_table_like(page)
    except Exception as e:
        return {"error": f"exception extracting text: {e}"}

    return {"error": "account not found on page after search attempt", "page_title": page.title()}

def download_pdf_via_request(playwright, url, dest_path):
    """
    Use the Playwright request API to GET the PDF URL and save it.
    This avoids rendering issues if the URL returns application/pdf.
    """
    rc = playwright.request.new_context()
    try:
        resp = rc.get(url, timeout=30000)
        if resp.status == 200:
            data = resp.body()
            dest_path.write_bytes(data)
            return {"status": "ok", "bytes": len(data)}
        else:
            return {"status": "error", "http_status": resp.status, "text": resp.text()[:1000]}
    finally:
        rc.dispose()

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # set True for headless runs
        context = browser.new_context()
        page = context.new_page()

        # ----- SCRAPE TrueProdigy propertySearch -----
        tp_url = "https://harris.trueprodigy-taxtransparency.com/taxTransparency/propertySearch"
        print("Opening TrueProdigy search page...")
        page.goto(tp_url, timeout=30000)
        # wait for a likely search box or content
        try:
            page.wait_for_selector("input, button, form", timeout=6000)
        except PlaywrightTimeout:
            pass

        tp_result = scrape_trueprodigy(page)
        out_tp = OUT_DIR / f"harris_trueprodigy_{ACCOUNT}.json"
        out_tp.write_text(json.dumps(tp_result, indent=2, ensure_ascii=False))
        print(f"Saved TrueProdigy scrape to {out_tp}")

        # ----- DOWNLOAD hctax.net PDF directly -----
        hctax_pdf_url = f"https://www.hctax.net/Property/pdf?t=print&Account={ACCOUNT}"
        out_pdf = OUT_DIR / f"hctax_{ACCOUNT}.pdf"
        print("Downloading hctax.net PDF...")
        pdf_result = download_pdf_via_request(p, hctax_pdf_url, out_pdf)
        print("PDF download result:", pdf_result)

        # cleanup
        context.close()
        browser.close()

if __name__ == "__main__":
    main()
