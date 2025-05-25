import os
import re
import random
import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright

# CONFIG
INPUT_PATH = "data.csv"
OUTPUT_PATH = "output_async.csv"
CONCURRENCY = 30
CHUNK_SIZE = 100
RETRIES = 3
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
}


def parse_view_count(text):
    try:
        text = text.upper().replace(",", "").strip()
        if "K" in text:
            return int(float(text.replace("K", "")) * 1_000)
        elif "M" in text:
            return int(float(text.replace("M", "")) * 1_000_000)
        elif "B" in text:
            return int(float(text.replace("B", "")) * 1_000_000_000)
        return int(text)
    except:
        return None


async def get_data_playwright(url):
    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=30000)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            title = soup.title.string.strip() if soup.title else None

            date_match = re.search(r"'video_date_published'\s*:\s*'(\d{8})'", html)
            upload_date = datetime.strptime(date_match.group(1), "%Y%m%d").date() if date_match else None

            votes_up = None
            span = soup.find("span", class_="votesUp")
            if span and span.has_attr("data-rating"):
                votes_up = int(span["data-rating"])

            views = None
            views_div = soup.find("div", class_="views")
            if views_div:
                count_span = views_div.find("span", class_="count")
                if count_span:
                    views = parse_view_count(count_span.text.strip())

            categories = []
            wrapper = soup.find("div", class_="categoriesWrapper")
            if wrapper:
                categories = [a.get_text(strip=True) for a in wrapper.find_all("a", class_="item")]

            tags = []
            meta = soup.find("meta", attrs={"name": "adsbytrafficjunkycontext"})
            if meta and meta.has_attr("data-context-tag"):
                tags = [t.strip() for t in meta["data-context-tag"].split(",")]

            await browser.close()
            return url, title, upload_date, votes_up, views, categories, tags

    except Exception as e:
        print(f"[Playwright Failed] {url} | {e}")
        return url, None, None, None, None, None, None


async def get_data(session, url, retries=RETRIES):
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=20) as resp:
                if resp.status != 200:
                    raise aiohttp.ClientResponseError(
                        status=resp.status,
                        message=f"HTTP {resp.status}",
                        request_info=resp.request_info,
                        history=resp.history
                    )

                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")

                title = soup.title.string.strip() if soup.title else None

                date_match = re.search(r"'video_date_published'\s*:\s*'(\d{8})'", html)
                upload_date = datetime.strptime(date_match.group(1), "%Y%m%d").date() if date_match else None

                votes_up = None
                votes_span = soup.find("span", class_="votesUp")
                if votes_span and votes_span.has_attr("data-rating"):
                    votes_up = int(votes_span["data-rating"])

                views = None
                views_div = soup.find("div", class_="views")
                if views_div:
                    count_span = views_div.find("span", class_="count")
                    if count_span:
                        views = parse_view_count(count_span.text.strip())

                categories = []
                wrapper = soup.find("div", class_="categoriesWrapper")
                if wrapper:
                    categories = [a.get_text(strip=True) for a in wrapper.find_all("a", class_="item")]

                tags = []
                meta = soup.find("meta", attrs={"name": "adsbytrafficjunkycontext"})
                if meta and meta.has_attr("data-context-tag"):
                    tags = [t.strip() for t in meta["data-context-tag"].split(",")]

                return url, title, upload_date, votes_up, views, categories, tags

        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                print(f"[aiohttp Failed] Falling back to Playwright: {url}")
                return await get_data_playwright(url)


async def run_scraper(urls, output_path):
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        for i in range(0, len(urls), CHUNK_SIZE):
            batch = urls[i:i + CHUNK_SIZE]
            print(f"\n▶ Processing batch {i // CHUNK_SIZE + 1} of {len(urls) // CHUNK_SIZE + 1}")

            tasks = [get_data(session, url) for url in batch]
            results = []
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Batch {i // CHUNK_SIZE + 1}"):
                result = await f
                results.append(result)

            batch_df = pd.DataFrame(results, columns=[
                "url", "_title", "_upload_date", "_votes_up", "_views", "_categories", "_tags"])
            batch_df["_categories"] = batch_df["_categories"].apply(lambda x: ";".join(x) if isinstance(x, list) else "")
            batch_df["_tags"] = batch_df["_tags"].apply(lambda x: ";".join(x) if isinstance(x, list) else "")

            write_header = not os.path.exists(output_path)
            batch_df.to_csv(output_path, mode="a", header=write_header, index=False)
            print(f"Appended {len(batch_df)} rows to {output_path}")

            await asyncio.sleep(random.uniform(0.8, 2.0))


def get_unprocessed_urls():
    df = pd.read_csv(INPUT_PATH, delimiter='‽', encoding='utf-8', engine='python')
    all_urls = set(df["url"].dropna())

    if os.path.exists(OUTPUT_PATH):
        existing = pd.read_csv(OUTPUT_PATH)
        done_urls = set(existing["url"].dropna())
        print(f"🔎 Found {len(done_urls)} already processed URLs.")
    else:
        done_urls = set()

    remaining = list(all_urls - done_urls)
    print(f"{len(remaining)} URLs left to process.")
    return remaining


def main():
    urls = get_unprocessed_urls()
    if urls:
        asyncio.run(run_scraper(urls, OUTPUT_PATH))
    else:
        print("✅ All URLs already processed.")


if __name__ == "__main__":
    main()
