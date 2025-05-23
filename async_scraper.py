import asyncio
import aiohttp
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm.asyncio import tqdm
import random

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
}

CONCURRENCY = 30  
CHUNK_SIZE = 100 


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

async def get_data(session, url):
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                return url, None, None, None, None, None

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # Date
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

            # Tags
            tags = []
            meta = soup.find("meta", attrs={"name": "adsbytrafficjunkycontext"})
            if meta and meta.has_attr("data-context-tag"):
                tags = [t.strip() for t in meta["data-context-tag"].split(",")]

            return url, upload_date, votes_up, views, categories, tags
    except Exception as e:
        error_message = f"{url} | {type(e).__name__}: {e}"
        print(f"Failed: {error_message}")
        async with asyncio.Lock():  # prevent race conditions when writing in parallel
            with open("failed_urls.log", "a", encoding="utf-8") as f:
                f.write(error_message + "\n")
        return url, None, None, None, None, None



async def run_scraper(urls):
    results = []
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        for i in range(0, len(urls), CHUNK_SIZE):
            batch = urls[i:i+CHUNK_SIZE]
            tasks = [get_data(session, url) for url in batch]
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Batch {i//CHUNK_SIZE+1}"):
                result = await f
                results.append(result)
            await asyncio.sleep(random.uniform(0.8, 2.0))

    return results


def main():
    df = pd.read_csv("data.csv", delimiter='‽', encoding='utf-8', engine='python')
    df = df.head(500)  

    urls = df["url"].tolist()
    results = asyncio.run(run_scraper(urls))

    # Unpack results
    result_df = pd.DataFrame(results, columns=["url", "upload_date", "votes_up", "views", "categories", "tags"])

    # Merge with original
    df = df.merge(result_df, on="url", how="left")

    # Serialize list columns
    df["categories"] = df["categories"].apply(lambda x: ";".join(x) if isinstance(x, list) else "")
    df["tags"] = df["tags"].apply(lambda x: ";".join(x) if isinstance(x, list) else "")

    df.to_csv("output_async.csv", index=False)
    print("Saved: output_async.csv")

if __name__ == "__main__":
    main()
