import requests
import re
from datetime import datetime

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
}

def get_upload_date_from_script(url):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None

        html = response.text

        # Find 'video_date_published': 'YYYYMMDD'
        match = re.search(r"'video_date_published'\s*:\s*'(\d{8})'", html)
        if match:
            raw_date = match.group(1)
            return datetime.strptime(raw_date, "%Y%m%d").date()

    except Exception as e:
        return None

    return None



