import pandas as pd
from scraper import get_upload_date
from tqdm import tqdm 
from time import sleep

tqdm.pandas()

df = pd.read_csv("porn-with-dates-2022.csv", low_memory=False)

df = df.head(100).copy()

if "upload_date" not in df.columns:
    df["upload_date"] = None

for i in tqdm(range(0, len(df), 100), desc="Scraping upload dates"):
    chunk = df.iloc[i:i+100]
    df.loc[i:i+100, "upload_date"] = chunk["url"].progress_apply(get_upload_date)
    sleep(1)  # be respectful — wait 1 second per chunk

df.to_csv("pornhub_with_upload_dates.csv", index=False)
