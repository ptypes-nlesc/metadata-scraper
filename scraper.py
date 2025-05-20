import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
}

def get_data(url):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None, None, None, None, None

        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        # --- Upload date ---
        date_match = re.search(r"'video_date_published'\s*:\s*'(\d{8})'", html)
        upload_date = None
        if date_match:
            raw_date = date_match.group(1)
            upload_date = datetime.strptime(raw_date, "%Y%m%d").date()

        # --- Votes up ---
        votes_up = None
        votes_span = soup.find("span", class_="votesUp")
        if votes_span and votes_span.has_attr("data-rating"):
            votes_up = int(votes_span["data-rating"])

        # --- Views ---
        views = None
        views_div = soup.find("div", class_="views")
        if views_div:
            count_span = views_div.find("span", class_="count")
            if count_span:
                views = parse_view_count(count_span.text.strip())

        # --- Categories ---
        categories = []
        categories_wrapper = soup.find("div", class_="categoriesWrapper")
        if categories_wrapper:
            links = categories_wrapper.find_all("a", class_="item")
            categories = [link.get_text(strip=True) for link in links]

        # --- Tags ---
        tags = []
        meta = soup.find("meta", attrs={"name": "adsbytrafficjunkycontext"})
        if meta and meta.has_attr("data-context-tag"):
            raw_tags = meta["data-context-tag"]
            tags = [t.strip() for t in raw_tags.split(",") if t.strip()]

        return upload_date, votes_up, views, categories, tags

    except Exception:
        return None, None, None, None, None

def parse_view_count(text):
    try:
        text = text.upper().replace(",", "").strip()
        if "K" in text:
            return int(float(text.replace("K", "")) * 1_000)
        elif "M" in text:
            return int(float(text.replace("M", "")) * 1_000_000)
        elif "B" in text:
            return int(float(text.replace("B", "")) * 1_000_000_000)
        else:
            return int(text)
    except:
        return None
