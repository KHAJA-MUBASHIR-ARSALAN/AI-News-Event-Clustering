import pandas as pd 
import numpy as np
from myeda import dataset_overview
from myeda import missing_overview, missing_summary
from myeda.core.missing import plot_missing


df = pd.read_csv("gdelt_2026_clean.csv")
pd.set_option("display.max_columns",None)
print(df.head(3))

# overview of dataset

print(dataset_overview(df))





# lower casing

def lowercase(df: pd.DataFrame) -> pd.DataFrame:
  text_cols = df.select_dtypes(include="object").columns


  for col in text_cols:
    df[col] = df[col].fillna("").str.lower()
  
  print("lowercasing is done.")
  return df

df = lowercase(df)

print(df.head(3))






# missing values
def missing(df: pd.DataFrame) -> pd.DataFrame:

  print("Handling missing values\n")

  print(missing_overview(df))
  print(missing_summary(df))
  plot_missing(df)

df = missing(df)

# here I tried for grouping country with their median lat long values but in 97% cases these values have null as both so, this strategy failed. If I go with whole dataset median that can be misslead my prediction.

print(df['ActionGeo_CountryCode'].isnull().sum())
df.groupby('ActionGeo_CountryCode')['ActionGeo_Lat'].count().sort_values()


df['ActionGeo_Lat'] = df['ActionGeo_Lat'].fillna(
    df.groupby('ActionGeo_CountryCode')['ActionGeo_Lat'].transform('median')
)

df['ActionGeo_Long'] = df['ActionGeo_Long'].fillna(
    df.groupby('ActionGeo_CountryCode')['ActionGeo_Long'].transform('median')
)

# so I simply drop those rows because they are just 2.5% of the dataset.

df = df.dropna(subset=['ActionGeo_Lat', 'ActionGeo_Long'], how='all')
df[['ActionGeo_Lat', 'ActionGeo_Long']].isnull().sum()

print("Data after handling missing values\n")

df = missing(df)





# converting into datetime
def datetime(df: pd.DataFrame) -> pd.DataFrame:
  print("converting datetime start")

  df['SQLDATE'] = pd.to_datetime(df['SQLDATE'], format='%Y%m%d')

  print(df.tail(3).T)
  print("converting datetime ended")

df = datetime(df)