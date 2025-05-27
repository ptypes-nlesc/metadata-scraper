import os
import re
import random
import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright  # <- Playwright fallback

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

def parse_html(html, url):
    soup = BeautifulSoup(html, "html.parser")

    # Upload date
    date_match = re.search(r"'video_date_published'\s*:\s*'(\d{8})'", html)
    upload_date = datetime.strptime(date_match.group(1), "%Y%m%d").date() if date_match else None

    # Votes up
    votes_up = None
    votes_span = soup.find("span", class_="votesUp")
    if votes_span and votes_span.has_attr("data-rating"):
        votes_up = int(votes_span["data-rating"])

    # Views
    views = None
    views_div = soup.find("div", class_="views")
    if views_div:
        count_span = views_div.find("span", class_="count")
        if count_span:
            views = parse_view_count(count_span.text.strip())

    # Categories
    categories = []
    wrapper = soup.find("div", class_="categoriesWrapper")
    if wrapper:
        categories = [a.get_text(strip=True) for a in wrapper.find_all("a", class_="item")]

    # Tagss
    tags = []
    meta = soup.find("meta", attrs={"name": "adsbytrafficjunkycontext"})
    if meta and meta.has_attr("data-context-tag"):
        tags = [t.strip() for t in meta["data-context-tag"].split(",")]

    # Title
    title_tag = soup.find("meta", attrs={"property": "og:title"})
    title = title_tag["content"].replace(" - Pornhub.com", "").strip() if title_tag else None

    return url, upload_date, votes_up, views, categories, tags, title

async def get_data_playwright(url):
    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            context = await browser.new_context(user_agent=HEADERS["User-Agent"])
            page = await context.new_page()
            await page.goto(url, timeout=30000)
            html = await page.content()
            await browser.close()
            return parse_html(html, url)
    except Exception as e:
        error_message = f"{url} | PLAYWRIGHT FAIL | {type(e).__name__}: {e}"
        print(error_message)
        async with asyncio.Lock():
            with open("failed_urls.log", "a", encoding="utf-8") as f:
                f.write(error_message + "\n")
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
                return parse_html(html, url)
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                print(f"Aiohttp failed: {url} | {type(e).__name__}: {e}")
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
                "url", "_upload_date", "_votes_up", "_views", "_categories", "_tags", "_title"
            ])
            batch_df["_categories"] = batch_df["_categories"].apply(lambda x: ";".join(x) if isinstance(x, list) else "")
            batch_df["_tags"] = batch_df["_tags"].apply(lambda x: ";".join(x) if isinstance(x, list) else "")

            write_header = not os.path.exists(output_path)
            batch_df.to_csv(output_path, mode="a", header=write_header, index=False)
            print(f"✔ Appended {len(batch_df)} rows to {output_path}")

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

def get_failed_urls():
    if not os.path.exists(OUTPUT_PATH):
        return []
    df = pd.read_csv(OUTPUT_PATH)
    failed_df = df[df[["_upload_date", "_votes_up", "_views", "_categories", "_tags", "_title"]].isnull().all(axis=1)]
    print(f"⚠ Found {len(failed_df)} rows with missing metadata.")
    return failed_df["url"].dropna().tolist()

def main():
    new_urls = get_unprocessed_urls()
    if new_urls:
        asyncio.run(run_scraper(new_urls, OUTPUT_PATH))

    retry_urls = get_failed_urls()
    if retry_urls:
        print("🔁 Retrying failed URLs...")
        temp_retry_path = "retry_temp.csv"
        asyncio.run(run_scraper(retry_urls, temp_retry_path))

        # Merge retries
        df_existing = pd.read_csv(OUTPUT_PATH).set_index("url")
        df_retry = pd.read_csv(temp_retry_path).set_index("url")
        df_merged = df_retry.combine_first(df_existing).reset_index()
        df_merged.to_csv(OUTPUT_PATH, index=False)
        os.remove(temp_retry_path)
        print("✅ Retried data merged into output.")

    print("✅ Scraping complete.")

if __name__ == "__main__":
    main()
