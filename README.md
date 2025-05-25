This asynchronous web scraper is designed to extract metadata from public Pornhub video pages.
It is developed for academic NLP and social computing research.

Steps:
1. Loads a CSV file (data.csv) with video URLs (delimited by ‽).
2. For each video URL, it retrieves:
  - Upload date
  - Vote count (upvotes)
  - View count
  - Categories
  - Tags
3. It runs requests concurrently (30 at a time by default), handles timeouts and errors, and writes failed URLs to a log.
4. Outputs a new CSV with the extracted metadata merged into the original dataset.


