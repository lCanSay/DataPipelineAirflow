import asyncio
import csv
from typing import List, Dict
from playwright.async_api import async_playwright

START_URL = "https://www.gog.com/en/games"
OUTPUT_CSV = "data/raw.csv"

async def extract_tile_data(tile) -> Dict:
    title_el = await tile.query_selector("product-title span")
    title = (await title_el.inner_text()) if title_el else None

    link_el = await tile.query_selector("a.product-tile")
    url = await link_el.get_attribute("href") if link_el else None
    product_id = await link_el.get_attribute("data-product-id") if link_el else None

    base_el = await tile.query_selector(".base-value")
    base_price = (await base_el.inner_text()) if base_el else None

    final_el = await tile.query_selector(".final-value")
    final_price = (await final_el.inner_text()) if final_el else None

    disc_el = await tile.query_selector("[selenium-id='productPriceDiscount']")
    discount = (await disc_el.inner_text()) if disc_el else None


    return {
        "product_id": product_id,
        "title": title,
        "url": url,
        "base_price": base_price,
        "final_price": final_price,
        "discount": discount,
    }

async def scrape_pages(max_pages: int = 10) -> List[Dict]:
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(START_URL, timeout=0)

        for page_no in range(1, max_pages + 1):
            print(f"[scraper] scraping page {page_no}")
            await page.wait_for_selector("product-tile", timeout=10000)

            tiles = await page.query_selector_all("product-tile")
            for tile in tiles:
                data = await extract_tile_data(tile)
                data["page"] = page_no
                results.append(data)

            next_btn = await page.query_selector("button[selenium-id='paginationNext']")
            if next_btn is None:
                break
            cls = await next_btn.get_attribute("class") or ""
            if "disabled" in cls:
                break
            await next_btn.click()
            await page.wait_for_timeout(1500)

        await browser.close()
    return results

def save_csv(rows, path=OUTPUT_CSV):
    if not rows:
        print("[scraper] no rows to save")
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[scraper] saved {len(rows)} rows to {path}")

if __name__ == "__main__":
    import sys
    pages = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    rows = asyncio.run(scrape_pages(max_pages=pages))
    save_csv(rows)
