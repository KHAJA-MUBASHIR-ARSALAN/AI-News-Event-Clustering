import pandas as pd 
import os

S3_PATH = "s3://ai-news-cluster/processed/gdelt_2026_clean.csv"
CACHE_PATH = "cache/gdelt_2026_clean.csv"

def load_df(use_cache=True) -> pd.DataFrame:
    if use_cache and os.path.exists(CACHE_PATH):
        print("Loading data from local cache")
        return pd.read_csv(CACHE_PATH)

    print("Loading data from S3")
    df = pd.read_csv(S3_PATH)

    os.makedirs("cache", exist_ok=True)
    df.to_csv(CACHE_PATH, index=False)
    return df