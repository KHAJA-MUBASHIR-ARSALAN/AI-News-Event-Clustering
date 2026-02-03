# import requests
# import os
# import glob
# import zipfile
# import pandas as pd
# import shutil
# # =========================
# # CONFIG
# # =========================
# MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
# SAVE_DIR = "gdelt_2026"
# FINAL_CSV = "gdelt_2026_clean.csv"

# # =========================
# # CLEAN OLD DATA
# # =========================


# if os.path.exists(FINAL_CSV):
#     os.remove(FINAL_CSV)


# os.makedirs(SAVE_DIR, exist_ok=True)

# # =========================
# # STEP 1: DOWNLOAD FILES
# # =========================
# print("Downloading GDELT 2026 files...")
# text = requests.get(MASTER_URL).text

# for line in text.splitlines():
#     parts = line.split()
#     if len(parts) < 3:
#         continue

#     url = parts[-1]

#     # ✅ STRICT filter: ONLY 2026 event files
#     if (
#         "/gdeltv2/2026" in url
#         and url.endswith(".export.CSV.zip")
#     ):
#         filename = os.path.basename(url)
#         filepath = os.path.join(SAVE_DIR, filename)

#         if os.path.exists(filepath):
#             continue

#         print(f"Downloading: {filename}")
#         r = requests.get(url, stream=True)
#         with open(filepath, "wb") as f:
#             for chunk in r.iter_content(8192):
#                 f.write(chunk)


# print("Download complete.")

# # =========================
# # STEP 2: UNZIP FILES
# # =========================
# print("Extracting zip files...")
# for zip_file in glob.glob(f"{SAVE_DIR}/*.zip"):
#     with zipfile.ZipFile(zip_file, "r") as z:
#         z.extractall(SAVE_DIR)

# print("Extraction complete.")

# # =========================
# # STEP 3: CLEAN + MERGE
# # =========================
# COL_INDEX_MAP = {
#     "GLOBALEVENTID": 0,
#     "SQLDATE": 1,
#     "Actor1Name": 6,
#     "Actor2Name": 16,
#     "EventCode": 26,
#     "EventBaseCode": 27,
#     "ActionGeo_FullName": 52,
#     "ActionGeo_CountryCode": 53,
#     "ActionGeo_Lat": 56,
#     "ActionGeo_Long": 57,
#     "SOURCEURL": 60
# }

# USE_COLS = list(COL_INDEX_MAP.values())
# COL_NAMES = list(COL_INDEX_MAP.keys())

# files = glob.glob(f"{SAVE_DIR}/*.export.CSV")
# print(f"Found {len(files)} CSV files")

# first = True

# for f in files:
#     df_part = pd.read_csv(
#         f,
#         sep="\t",
#         header=None,
#         usecols=USE_COLS,
#         names=COL_NAMES,
#         low_memory=False
#     )

#     df_part.to_csv(
#         FINAL_CSV,
#         mode="w" if first else "a",
#         index=False,
#         header=first
#     )
#     first = False

# print("✅ Finished writing gdelt_2026_clean.csv")




import requests
import os
import glob
import zipfile
import pandas as pd
import boto3
import shutil
from botocore.exceptions import ClientError

# =========================
# CONFIG
# =========================
MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
LOCAL_DIR = "gdelt_temp"

BUCKET = "ai-news-cluster"
S3_PROCESSED_CSV = "processed/gdelt_2026_clean.csv"
S3_STATE_FILE = "state/processed_files.txt"

os.makedirs(LOCAL_DIR, exist_ok=True)
s3 = boto3.client("s3")

# =========================
# LOAD PROCESSED FILES (FROM S3)
# =========================
def load_processed_files():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=S3_STATE_FILE)
        content = obj["Body"].read().decode().strip()
        if not content:
            return set()
        return set(content.splitlines())

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print("No processed state file found in S3 (first run).")
            return set()
        else:
            raise

def save_processed_files(files):
    s3.put_object(
        Bucket=BUCKET,
        Key=S3_STATE_FILE,
        Body="\n".join(sorted(files))
    )

# =========================
# DOWNLOAD ONLY NEW FILES
# =========================
processed = load_processed_files()
new_processed = set(processed)

print("Checking for new GDELT files...")
text = requests.get(MASTER_URL, timeout=30).text

for line in text.splitlines():
    parts = line.split()
    if len(parts) < 3:
        continue

    url = parts[-1]
    filename = os.path.basename(url)
    local_path = os.path.join(LOCAL_DIR, filename)

    # 🚫 Skip if already processed in S3 OR exists locally
    if filename in processed or os.path.exists(local_path):
        continue

    if "/gdeltv2/2026" in url and url.endswith(".export.CSV.zip"):
        print(f"Downloading: {filename}")

        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        new_processed.add(filename)

# =========================
# EXIT EARLY IF NOTHING NEW
# =========================
if new_processed == processed:
    print("No new files found. Exiting.")
    shutil.rmtree(LOCAL_DIR)
    exit(0)

# =========================
# UNZIP FILES
# =========================
for zip_file in glob.glob(f"{LOCAL_DIR}/*.zip"):
    with zipfile.ZipFile(zip_file, "r") as z:
        z.extractall(LOCAL_DIR)

# =========================
# MERGE DATA
# =========================
COL_INDEX_MAP = {
    "GLOBALEVENTID": 0,
    "SQLDATE": 1,
    "Actor1Name": 6,
    "Actor2Name": 16,
    "EventCode": 26,
    "EventBaseCode": 27,
    "ActionGeo_FullName": 52,
    "ActionGeo_CountryCode": 53,
    "SOURCEURL": 60
}

USE_COLS = list(COL_INDEX_MAP.values())
COL_NAMES = list(COL_INDEX_MAP.keys())

# Load existing merged CSV from S3
try:
    existing_df = pd.read_csv(f"s3://{BUCKET}/{S3_PROCESSED_CSV}")
except Exception:
    existing_df = pd.DataFrame(columns=COL_NAMES)

files = glob.glob(f"{LOCAL_DIR}/*.export.CSV")
dfs = []

for f in files:
    df = pd.read_csv(
        f,
        sep="\t",
        header=None,
        usecols=USE_COLS,
        names=COL_NAMES,
        low_memory=False
    )
    dfs.append(df)

if dfs:
    new_df = pd.concat(dfs, ignore_index=True)
    final_df = pd.concat([existing_df, new_df], ignore_index=True)
else:
    final_df = existing_df

# =========================
# UPLOAD TO S3
# =========================
final_df.to_csv(
    f"s3://{BUCKET}/{S3_PROCESSED_CSV}",
    index=False
)

save_processed_files(new_processed)

print("Uploaded merged CSV and updated state file in S3")

# =========================
# CLEAN LOCAL
# =========================
shutil.rmtree(LOCAL_DIR)
print("Local temp directory removed")


