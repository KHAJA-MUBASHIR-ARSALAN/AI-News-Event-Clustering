import requests
import os
import glob
import zipfile
import pandas as pd
import shutil
# =========================
# CONFIG
# =========================
MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
SAVE_DIR = "gdelt_2026"
FINAL_CSV = "gdelt_2026_clean.csv"

# =========================
# CLEAN OLD DATA
# =========================


if os.path.exists(FINAL_CSV):
    os.remove(FINAL_CSV)


os.makedirs(SAVE_DIR, exist_ok=True)

# =========================
# STEP 1: DOWNLOAD FILES
# =========================
print("Downloading GDELT 2026 files...")
text = requests.get(MASTER_URL).text

for line in text.splitlines():
    parts = line.split()
    if len(parts) < 3:
        continue

    url = parts[-1]

    # ✅ STRICT filter: ONLY 2026 event files
    if (
        "/gdeltv2/2026" in url
        and url.endswith(".export.CSV.zip")
    ):
        filename = os.path.basename(url)
        filepath = os.path.join(SAVE_DIR, filename)

        if os.path.exists(filepath):
            continue

        print(f"Downloading: {filename}")
        r = requests.get(url, stream=True)
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)


print("Download complete.")

# =========================
# STEP 2: UNZIP FILES
# =========================
print("Extracting zip files...")
for zip_file in glob.glob(f"{SAVE_DIR}/*.zip"):
    with zipfile.ZipFile(zip_file, "r") as z:
        z.extractall(SAVE_DIR)

print("Extraction complete.")

# =========================
# STEP 3: CLEAN + MERGE
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
    "ActionGeo_Lat": 56,
    "ActionGeo_Long": 57,
    "SOURCEURL": 60
}

USE_COLS = list(COL_INDEX_MAP.values())
COL_NAMES = list(COL_INDEX_MAP.keys())

files = glob.glob(f"{SAVE_DIR}/*.export.CSV")
print(f"Found {len(files)} CSV files")

first = True

for f in files:
    df_part = pd.read_csv(
        f,
        sep="\t",
        header=None,
        usecols=USE_COLS,
        names=COL_NAMES,
        low_memory=False
    )

    df_part.to_csv(
        FINAL_CSV,
        mode="w" if first else "a",
        index=False,
        header=first
    )
    first = False

print("✅ Finished writing gdelt_2026_clean.csv")
