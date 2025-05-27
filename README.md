This asynchronous web scraper is designed to extract metadata from public Pornhub video pages.
It is developed for academic NLP and social computing research.

Steps:
1. Load a CSV file (data.csv) with video URLs (delimited by ‽).
2. For each video URL retrieve:
  - Title
  - Upload date
  - Vote count (upvotes)
  - View count
  - Categories
  - Tags
3. Use aiohttp for fast concurrent scraping (primary method).
4. Retry failed aiohttp requests with exponential backoff.
5. Fall back to Playwright if aiohttp fails after all retries.
6. Log hard failures if even Playwright can't scrape.
7. Append results in batches to .csv, avoiding full reloads.
8. Retry failed rows from the CSV using a two-pass strategy.

