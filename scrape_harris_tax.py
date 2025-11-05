"""
Playwright scraping script for:
  1) https://harris.trueprodigy-taxtransparency.com/taxTransparency/propertySearch
  2) https://www.hctax.net/Property/ViewStatementReceipts

Outputs:
  - harris_trueprodigy_{ACCOUNT}.json              (parsed TrueProdigy data, if found)
  - hctax_statement_{ACCOUNT}_{STATEMENT_YEAR}.json (parsed statement data, if found)
  - hctax_statement_{ACCOUNT}_{STATEMENT_YEAR}.pdf  (downloaded PDF from hctax.net)
"""

import json
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

ACCOUNT = "0552850000031"
STATEMENT_YEAR = "2024"
OUT_DIR = Path(".")
OUT_DIR.mkdir(exist_ok=True)
LOG_FILE = OUT_DIR / "scrape_errors.log"

def log_event(message, level="INFO"):
    timestamp = datetime.utcnow().isoformat() + "Z"
    try:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(f"{timestamp} [{level}] {message}\n")
    except Exception:
        # logging should never break the scraping flow
        pass

def log_error(message):
    log_event(message, level="ERROR")

def raise_runtime_error(message, cause=None):
    log_error(message)
    if cause:
        raise RuntimeError(message) from cause
    raise RuntimeError(message)

def try_fill_selectors(page, selectors, value, timeout=3000):
    """
    Try multiple selectors until one works. Returns True if filled.
    """
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            locator.wait_for(state="visible", timeout=timeout)
            if locator.is_disabled():
                continue
            locator.click()
            locator.fill(value)
            return True
        except PlaywrightTimeout:
            continue
        except AttributeError:
            # running on older Playwright versions without locator helpers
            try:
                if page.query_selector(sel):
                    page.fill(sel, value, timeout=timeout)
                    return True
            except Exception:
                continue
        except Exception:
            # some selectors may raise if not present, skip
            continue
    return False

def try_click_selectors(page, selectors, timeout=5000):
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            locator.wait_for(state="visible", timeout=timeout)
            if hasattr(locator, "is_disabled") and locator.is_disabled():
                continue
            locator.click(timeout=timeout)
            return True
        except PlaywrightTimeout:
            continue
        except AttributeError:
            try:
                if page.query_selector(sel):
                    page.click(sel, timeout=timeout)
                    return True
            except Exception:
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

def structure_table_entry(entry):
    """
    Convert a raw table entry (title + rows of cells) into a structured object
    with key/value fields where possible and tabular records otherwise.
    """
    title = entry.get("title", "").strip()
    structured = {}
    if title:
        structured["title"] = title

    headers = None
    fields = {}
    records = []
    residual_rows = []

    for row in entry.get("rows", []):
        cells = [cell.strip() for cell in row.get("cells", []) if cell and cell.strip()]
        if not cells:
            continue
        if row.get("header"):
            headers = cells
            continue
        if headers and len(cells) == len(headers):
            records.append(dict(zip(headers, cells)))
            continue
        if len(cells) == 2:
            key = cells[0].rstrip(":")
            value = cells[1]
            if key in fields:
                if isinstance(fields[key], list):
                    fields[key].append(value)
                else:
                    fields[key] = [fields[key], value]
            else:
                fields[key] = value
            continue
        residual_rows.append(cells)

    if headers:
        structured["headers"] = headers
    if fields:
        structured["fields"] = fields
    if records:
        structured["records"] = records
    if residual_rows:
        structured["rows"] = residual_rows

    return structured

def normalize_table_entries(raw_tables):
    tables = []
    for entry in raw_tables:
        structured = structure_table_entry(entry)
        if structured:
            tables.append(structured)
    return tables

def extract_tables_from_selector(page, selector):
    """
    Extract raw table data from a selector and return normalized entries.
    """
    raw_tables = page.evaluate(
        """(sel) => {
            const container = document.querySelector(sel);
            if (!container) return null;
            const tables = [];
            const tableElements = Array.from(container.matches('table') ? [container] : container.querySelectorAll('table'));
            tableElements.forEach(table => {
                let title = '';
                const caption = table.querySelector('caption');
                if (caption) {
                    title = caption.innerText.trim();
                }
                if (!title) {
                    let prev = table.previousElementSibling;
                    while (prev) {
                        const text = (prev.innerText || '').trim();
                        if (text && text.length < 200) {
                            title = text;
                            break;
                        }
                        prev = prev.previousElementSibling;
                    }
                }
                const rows = Array.from(table.querySelectorAll('tr')).map(tr => {
                    const cells = Array.from(tr.querySelectorAll('th, td')).map(td => td.innerText.trim());
                    const header = tr.querySelectorAll('th').length > 0;
                    return {cells, header};
                }).filter(row => row.cells.some(Boolean));
                tables.push({title, rows});
            });
            return tables;
        }""",
        selector,
    )
    if not raw_tables:
        return None
    return normalize_table_entries(raw_tables)

def extract_key_value_section(page, selector):
    """
    Split text content in a container into key/value pairs where possible, plus residual rows.
    """
    section = page.evaluate(
        """(sel) => {
            const container = document.querySelector(sel);
            if (!container) return null;
            const rawLines = container.innerText.split('\\n').map(t => t.trim()).filter(Boolean);
            const keyValues = {};
            const rows = [];
            rawLines.forEach(line => {
                const idx = line.indexOf(':');
                if (idx > 0 && idx < line.length - 1) {
                    const key = line.slice(0, idx).trim();
                    const value = line.slice(idx + 1).trim();
                    if (key in keyValues) {
                        const existing = keyValues[key];
                        if (Array.isArray(existing)) {
                            existing.push(value);
                        } else {
                            keyValues[key] = [existing, value];
                        }
                    } else {
                        keyValues[key] = value;
                    }
                } else {
                    rows.push(line);
                }
            });
            return {key_values: keyValues, rows, lines: rawLines};
        }""",
        selector,
    )
    if not section:
        return None
    if not section.get("key_values"):
        section.pop("key_values", None)
    if not section.get("rows"):
        section.pop("rows", None)
    if not section.get("lines"):
        section.pop("lines", None)
    return section

def extract_jurisdiction_containers(page, selector=".middle-container"):
    """
    Extract multiple jurisdiction entries from the middle container.
    Each .custom-container inside is treated as a separate jurisdiction block.
    """
    data = page.evaluate(
        """(sel) => {
            const root = document.querySelector(sel);
            if (!root) return null;
            const containers = Array.from(root.querySelectorAll('.custom-container'));
            return containers.map(container => {
                const result = {};
                const heading = container.querySelector('h1,h2,h3,h4,h5,strong,.title');
                if (heading) {
                    result.label = heading.innerText.trim();
                }

                const rawLines = container.innerText.split('\\n')
                    .map(line => line.trim())
                    .filter(Boolean);

                const keyValues = {};
                const lines = [];
                rawLines.forEach(line => {
                    if (heading && line === heading.innerText.trim()) {
                        return;
                    }
                    const idx = line.indexOf(':');
                    if (idx > 0 && idx < line.length - 1) {
                        const key = line.slice(0, idx).trim();
                        const value = line.slice(idx + 1).trim();
                        if (keyValues[key]) {
                            if (Array.isArray(keyValues[key])) {
                                keyValues[key].push(value);
                            } else {
                                keyValues[key] = [keyValues[key], value];
                            }
                        } else {
                            keyValues[key] = value;
                        }
                    } else {
                        lines.push(line);
                    }
                });

                if (Object.keys(keyValues).length) {
                    result.fields = keyValues;
                }
                if (lines.length) {
                    result.lines = lines;
                }

                const tables = Array.from(container.querySelectorAll('table')).map(table => {
                    const headers = Array.from(table.querySelectorAll('th')).map(th => th.innerText.trim());
                    const rows = Array.from(table.querySelectorAll('tr')).map(tr => {
                        const cells = Array.from(tr.querySelectorAll('th, td')).map(td => td.innerText.trim());
                        return cells.filter(Boolean);
                    }).filter(row => row.length);
                    return {headers, rows};
                }).filter(table => table.rows.length);

                if (tables.length) {
                    result.tables = tables;
                }

                return result;
            });
        }""",
        selector,
    )
    return data or None

def scrape_hctax_statement(page, playwright, account=ACCOUNT, statement_year=STATEMENT_YEAR):
    """
    Navigate to the Harris County Tax Office ViewStatementReceipts page, search
    for an account, drill into the statement, extract table data, and download the PDF.
    """
    base_url = "https://www.hctax.net/Property/ViewStatementReceipts"
    print("Opening hctax.net ViewStatementReceipts page...")
    page.goto(base_url, timeout=30000)

    input_selectors = [
        "input#txtSearchValue",
        "input#SearchAccount",
        "input[name='SearchAccount']",
        "input[name='AccountNumber']",
        "input[placeholder*='Account']",
        "input[type='search']",
        "input[type='text']"
    ]
    filled = try_fill_selectors(page, input_selectors, account)
    if not filled:
        raise_runtime_error("Unable to locate account search input on hctax.net")

    clicked = try_click_selectors(page, [
        "button#btnSubmitTaxSearch",
        "button#SearchButton",
        "button[type='submit']",
        "button:has-text('Search')",
        "input[type='submit']",
        "a:has-text('Search')"
    ])
    if not clicked:
        # fallback: press Enter
        page.keyboard.press("Enter")

    try:
        account_link_selector = f"a:has-text('{account}')"
        page.wait_for_selector(account_link_selector, timeout=12000)
        page.click(account_link_selector)
    except PlaywrightTimeout as exc:
        raise_runtime_error(f"Account link for {account} not found after search", exc)

    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except PlaywrightTimeout:
        pass
    time.sleep(1.0)

    statement_heading_selector = f"text={statement_year} Property Tax Statement"
    try:
        heading = page.wait_for_selector(statement_heading_selector, timeout=12000)
    except PlaywrightTimeout as exc:
        raise_runtime_error(f"Could not locate heading '{statement_year} Property Tax Statement'", exc)

    container_handle = heading.evaluate_handle(
        """(el) => el.closest('.card, .panel, section, .modal, .row, .col-12, .container') || el.parentElement"""
    )
    raw_tables = container_handle.evaluate(
        """(container) => {
            const result = [];
            const tables = Array.from(container.querySelectorAll('table'));
            tables.forEach(table => {
                let title = '';
                const caption = table.querySelector('caption');
                if (caption) {
                    title = caption.innerText.trim();
                }
                if (!title) {
                    let prev = table.previousElementSibling;
                    while (prev) {
                        const text = (prev.innerText || '').trim();
                        if (text && text.length < 200) {
                            title = text;
                            break;
                        }
                        prev = prev.previousElementSibling;
                    }
                }
                const rows = Array.from(table.querySelectorAll('tr')).map(tr => {
                    const cells = Array.from(tr.querySelectorAll('th, td')).map(td => td.innerText.trim());
                    const header = Array.from(tr.querySelectorAll('th')).length > 0;
                    return {cells, header};
                }).filter(row => row.cells.some(cell => cell));
                result.push({title, rows});
            });
            return result;
        }"""
    )

    paragraph_text = container_handle.evaluate(
        """(container) => {
            const paragraphs = Array.from(container.querySelectorAll('p'));
            const texts = paragraphs.map(p => p.innerText.trim()).filter(Boolean);
            return Array.from(new Set(texts));
        }"""
    )

    statement_data = {
        "account": account,
        "statement_year": statement_year,
        "url": page.url,
        "tables": normalize_table_entries(raw_tables),
    }
    if paragraph_text:
        statement_data["paragraphs"] = paragraph_text

    pdf_path = OUT_DIR / f"hctax_statement_{account}_{statement_year}.pdf"
    pdf_status = download_statement_pdf(page, playwright, pdf_path)
    statement_data["pdf"] = {
        "path": str(pdf_path),
        **pdf_status
    }

    return statement_data

def find_and_click_account(page, account):
    """
    After submitting a search, locate the requested account in the results and drill down.
    """
    possible_selectors = [
        f"a:has-text('{account}')",
        f"button:has-text('{account}')",
        f"text={account}",
        f"tr:has-text('{account}')",
        f"div:has-text('{account}')",
    ]
    for sel in possible_selectors:
        try:
            locator = page.locator(sel).first
            locator.wait_for(state="visible", timeout=8000)
            locator.click(timeout=3000)
            return True
        except PlaywrightTimeout:
            continue
        except Exception:
            continue
    return False

def scrape_trueprodigy(page, account=ACCOUNT):
    """
    Attempt to search by Account # and extract result.
    This site uses a search form — selectors may change; we try several options.
    """
    # heuristics: try common input names/ids
    possible_inputs = [
        "input[name='account']", "input[name='Account']", "input#Account",
        "input[name='parcel']", "input[name='searchText']", "input[type='text']",
        "input[placeholder*='Account']", "input[placeholder*='Search']",
        "input[placeholder*='Search by Name']"
    ]
    filled = try_fill_selectors(page, possible_inputs, account)
    if not filled:
        # try to fill any visible text inputs (first match)
        try:
            text_inputs = page.query_selector_all("input[type='text'], input:not([type])")
            if text_inputs:
                # fill first visible one
                for ti in text_inputs:
                    try:
                        if ti.is_visible():
                            ti.fill(account)
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
        "button:has-text('SEARCH')",
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
        pass

    # give JS a moment to render results
    time.sleep(1.0)

    if not find_and_click_account(page, account):
        message = f"Account {account} not found in TrueProdigy search results"
        log_error(message)
        return {"error": message, "page_title": page.title()}

    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except PlaywrightTimeout:
        pass

    property_summary_selectors = [
        ".property-summary-container.custom-container",
        ".property-summary-container",
        ".summary-container"
    ]
    try:
        page.wait_for_selector(", ".join(property_summary_selectors), timeout=12000)
    except PlaywrightTimeout:
        message = "Property summary container not found after navigating to detail page"
        log_error(message)
        return {"error": message}

    property_summary = None
    for sel in property_summary_selectors:
        property_summary = extract_key_value_section(page, sel)
        if property_summary:
            property_summary["selector"] = sel
            break
    tax_summary = extract_tables_from_selector(page, "#propertys-summary-table") or extract_tables_from_selector(page, ".propertys-summary-table")
    jurisdiction_container = extract_key_value_section(page, ".middle-container")
    jurisdictions = extract_jurisdiction_containers(page, ".middle-container")
    jurisdiction_summary = extract_tables_from_selector(page, ".middle-container")

    result = {
        "account": account,
        "url": page.url,
    }
    if property_summary:
        result["property_summary"] = property_summary
    if tax_summary:
        result["tax_summary"] = tax_summary
    if jurisdiction_container:
        result["jurisdiction_details"] = jurisdiction_container
    if jurisdictions:
        result["jurisdictions"] = jurisdictions
    if jurisdiction_summary:
        result["jurisdiction_summary"] = jurisdiction_summary

    if not (property_summary or tax_summary or jurisdiction_summary):
        # fallback to generic extraction
        result["fallback"] = extract_table_like(page)

    return result

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

def download_statement_pdf(page, playwright, dest_path):
    """
    Click the 'Print Statement' control and save the resulting PDF.
    Falls back to request download if a popup is opened.
    """
    button_selectors = [
        "a:has-text('Print Statement')",
        "button:has-text('Print Statement')",
        "text=Print Statement"
    ]

    for sel in button_selectors:
        try:
            page.wait_for_selector(sel, timeout=4000)
        except PlaywrightTimeout:
            continue

        # attempt direct download
        try:
            with page.expect_download(timeout=15000) as download_info:
                page.click(sel, timeout=2000)
            download = download_info.value
            suggested = download.suggested_filename
            download.save_as(str(dest_path))
            return {"status": "ok", "source": "download_event", "suggested_filename": suggested}
        except PlaywrightTimeout:
            pass
        except Exception as err:
            return {"status": "error", "message": f"download failed: {err}"}

        # attempt popup download
        try:
            with page.expect_popup(timeout=8000) as popup_info:
                page.click(sel, timeout=2000)
            popup = popup_info.value
            popup.wait_for_load_state("load", timeout=10000)
            pdf_result = download_pdf_via_request(playwright, popup.url, dest_path)
            popup.close()
            if pdf_result.get("status") == "ok":
                pdf_result["source"] = "popup_request"
            return pdf_result
        except PlaywrightTimeout:
            continue
        except Exception as err:
            return {"status": "error", "message": f"popup download failed: {err}"}

    return {"status": "error", "message": "Print Statement control not found"}

def main():
    start_time = time.time()
    out_tp = OUT_DIR / f"harris_trueprodigy_{ACCOUNT}.json"
    out_statement = OUT_DIR / f"hctax_statement_{ACCOUNT}_{STATEMENT_YEAR}.json"
    out_pdf = OUT_DIR / f"hctax_statement_{ACCOUNT}_{STATEMENT_YEAR}.pdf"
    log_event(f"Run start account={ACCOUNT} statement_year={STATEMENT_YEAR}")

    tp_duration = None
    hctax_duration = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)  # set True for headless runs
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            try:
                # ----- SCRAPE TrueProdigy propertySearch -----
                tp_url = "https://harris.trueprodigy-taxtransparency.com/taxTransparency/propertySearch"
                print("Opening TrueProdigy search page...")
                tp_start = time.time()
                page.goto(tp_url, timeout=30000)
                # wait for a likely search box or content
                try:
                    page.wait_for_selector("input, button, form", timeout=6000)
                except PlaywrightTimeout:
                    pass

                try:
                    tp_result = scrape_trueprodigy(page)
                    out_tp.write_text(json.dumps(tp_result, indent=2, ensure_ascii=False))
                    print(f"Saved TrueProdigy scrape to {out_tp}")
                finally:
                    tp_duration = time.time() - tp_start
                    log_event(f"TrueProdigy scrape duration={tp_duration:.2f}s")

                # ----- DOWNLOAD hctax.net PDF directly -----
                print("Capturing hctax.net statement...")
                hctax_start = time.time()
                try:
                    hctax_statement = scrape_hctax_statement(page, p, ACCOUNT, STATEMENT_YEAR)
                except Exception as exc:
                    hctax_statement = {"error": str(exc), "account": ACCOUNT, "statement_year": STATEMENT_YEAR}
                    print("Error capturing hctax statement:", exc)
                    log_error(f"hctax statement error for account {ACCOUNT}: {exc}")

                out_statement.write_text(json.dumps(hctax_statement, indent=2, ensure_ascii=False))
                print(f"Saved hctax statement data to {out_statement}")
                hctax_duration = time.time() - hctax_start
                log_event(f"hctax statement scrape duration={hctax_duration:.2f}s")
            finally:
                context.close()
                browser.close()
    except Exception as exc:
        log_error(f"Unhandled exception: {exc}")
        raise
    finally:
        duration = time.time() - start_time
        outputs = []
        for path in [out_tp, out_statement, out_pdf]:
            try:
                if path.exists():
                    outputs.append(f"{path.name}={path.stat().st_size}B")
            except Exception:
                continue
        output_summary = ", ".join(outputs) if outputs else "none"
        summary_parts = [
            f"runtime_total={duration:.2f}s",
            f"tp_duration={tp_duration:.2f}s" if tp_duration is not None else "tp_duration=n/a",
            f"hctax_duration={hctax_duration:.2f}s" if hctax_duration is not None else "hctax_duration=n/a",
            f"outputs={output_summary}",
        ]
        log_event(
            f"Run end account={ACCOUNT} statement_year={STATEMENT_YEAR} "
            + " ".join(summary_parts)
        )

if __name__ == "__main__":
    main()
