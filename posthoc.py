import pandas as pd

# Load your output file
df = pd.read_csv("output_async.csv")

# Check which rows have *any* metadata successfully extracted
has_metadata = df[["_upload_date", "_votes_up", "_views", "_categories", "_tags"]].notnull().any(axis=1)

# Count total and successful rows
total_rows = len(df)
successful_rows = has_metadata.sum()
failed_rows = total_rows - successful_rows

print(f"Total videos: {total_rows}")
print(f"Videos with metadata: {successful_rows}")
print(f"Videos with no metadata: {failed_rows}")
