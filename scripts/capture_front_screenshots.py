#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def shot(page, out_dir: Path, name: str, wait_ms: int = 1200) -> None:
    page.wait_for_timeout(wait_ms)
    page.screenshot(path=str(out_dir / name), full_page=True)


def click_if_present(page, selector: str, wait_ms: int = 900) -> bool:
    node = page.query_selector(selector)
    if not node:
        return False
    node.click()
    page.wait_for_timeout(wait_ms)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture frontend screenshots for README gallery.")
    parser.add_argument("--front-url", default=os.getenv("FRONT_URL", "http://127.0.0.1:5173"))
    parser.add_argument("--api-url", default=os.getenv("API_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--api-key", default=os.getenv("API_KEY", ""))
    parser.add_argument(
        "--out-dir",
        default=os.getenv("OUT_DIR", str(Path("docs/media/screenshots"))),
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1720, "height": 980}, device_scale_factor=1)
        context.add_init_script("window.localStorage.setItem('simple_front.language','en-US');")
        page = context.new_page()
        try:
            page.goto(args.front_url, wait_until="domcontentloaded", timeout=30000)
            # Force English UI and show language options in a single screenshot.
            if page.query_selector("#loginLanguage"):
                page.select_option("#loginLanguage", "en-US")
                page.eval_on_selector("#loginLanguage", "el => { el.size = 3; el.style.minHeight = '96px'; }")
            shot(page, out_dir, "01-login-language-options.png")
            if page.query_selector("#loginLanguage"):
                page.eval_on_selector("#loginLanguage", "el => { el.size = 1; el.style.minHeight = ''; }")

            if args.api_key:
                page.fill("#baseUrl", args.api_url)
                page.fill("#apiKey", args.api_key)
                page.click("#loginAuthBtn")
                page.wait_for_timeout(2500)

                if click_if_present(page, "#tabCommandsBtn", 1200):
                    shot(page, out_dir, "02-oms-commands.png", 400)
                if click_if_present(page, "#tabPositionsBtn", 1200):
                    shot(page, out_dir, "03-oms-positions.png", 400)
                    click_if_present(page, 'wa-tab[panel="symbolsList"]', 1200)
                    account_id = "1"
                    if page.query_selector("#sendAccountId"):
                        raw = (page.input_value("#sendAccountId") or "").strip()
                        if raw:
                            account_id = raw.split()[0]
                    elif page.query_selector("#viewAccountsSelect option"):
                        first = page.query_selector("#viewAccountsSelect option")
                        if first:
                            value = (first.get_attribute("value") or "").strip()
                            if value:
                                account_id = value
                    if page.query_selector("#omsSymbolsAccountId"):
                        page.fill("#omsSymbolsAccountId", account_id)
                    click_if_present(page, "#refreshOmsSymbolsBtn", 900)
                    try:
                        page.wait_for_selector("#omsSymbolsTable .tabulator-row", timeout=12000)
                    except PlaywrightTimeoutError:
                        pass
                    shot(page, out_dir, "04-oms-symbol-list.png", 500)
                if click_if_present(page, "#tabSystemBtn", 1200):
                    shot(page, out_dir, "05-system-ccxt-orders.png", 500)
                    click_if_present(page, 'wa-tab[panel="ccxtTrades"]', 1200)
                    shot(page, out_dir, "06-system-ccxt-trades.png", 500)
                if click_if_present(page, "#tabAdminGroupBtn", 300):
                    click_if_present(page, "#tabAdminBtn", 1200)
                    shot(page, out_dir, "07-admin-accounts.png", 400)
                    click_if_present(page, "#tabAdminApiKeysBtn", 1200)
                    shot(page, out_dir, "08-admin-api-keys.png", 400)
                    click_if_present(page, "#tabAdminStatusBtn", 1600)
                    shot(page, out_dir, "09-admin-system-status.png", 300)
                if click_if_present(page, "#tabRiskGroupBtn", 300):
                    click_if_present(page, "#tabRiskAccountsBtn", 1300)
                    shot(page, out_dir, "10-risk-accounts.png", 300)

            print(f"Screenshots saved to: {out_dir.resolve()}")
            return 0
        except PlaywrightTimeoutError as exc:
            print(f"Timeout while capturing screenshots: {exc}")
            return 2
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
