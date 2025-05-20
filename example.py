import pandas as pd
from scraper import get_data
from tqdm import tqdm
from time import sleep

tqdm.pandas()

# Load and limit to first 100 rows
df = pd.read_csv("data.csv", delimiter='‽', encoding='utf-8')
df = df.head(100).copy()

# Ensure all output columns exist
for col in ["upload_date", "votes_up", "views", "categories", "tags"]:
    if col not in df.columns:
        df[col] = None

# Apply get_data and unpack all return values
def extract_metadata(url):
    upload_date, votes_up, views, categories, tags = get_data(url)
    return pd.Series({
        "upload_date": upload_date,
        "votes_up": votes_up,
        "views": views,
        "categories": categories,
        "tags": tags
    })

# Process in chunks of 100
for i in tqdm(range(0, len(df), 100), desc="Scraping metadata"):
    chunk = df.iloc[i:i+100]
    result = chunk["url"].progress_apply(extract_metadata)
    df.loc[i:i+100, ["upload_date", "votes_up", "views", "categories", "tags"]] = result.values
    sleep(1)  # be respectful to the server

# Save to file
df.to_csv("output.csv", index=False)
