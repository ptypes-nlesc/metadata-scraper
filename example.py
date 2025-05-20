import pandas as pd
from scraper import get_data
from tqdm import tqdm
from time import sleep
import random
from concurrent.futures import ThreadPoolExecutor

tqdm.pandas()

# Load and limit to first 100 rows
df = pd.read_csv("data.csv", delimiter='‽', encoding='utf-8')
df = df.head(200).copy()

MAX_WORKERS = 5

for i in tqdm(range(0, len(df), 100), desc="Scraping metadata"):
    chunk = df.iloc[i:i+100]

    if chunk.empty:
        continue

    idx = chunk.index
    urls = chunk["url"].tolist()

    # Run get_data(url) in parallel across the chunk
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(tqdm(executor.map(get_data, urls), total=len(urls), desc="Threaded chunk"))

    # Convert results to DataFrame
    result_df = pd.DataFrame(results, columns=["upload_date", "votes_up", "views", "categories", "tags"])
    df.loc[idx, ["upload_date", "votes_up", "views", "categories", "tags"]] = result_df.values

    # Save progress
    df.to_csv("output.csv", index=False)
    sleep(random.uniform(0.8, 1.5))