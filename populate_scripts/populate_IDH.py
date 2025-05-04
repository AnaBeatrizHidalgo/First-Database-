import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_IDH      = Path("Datasets/human-development-index-vs-gdp-per-capita.csv")
PATH_GMPI_T1  = Path("Datasets/2024_gMPI_Table1and2 - gMPI_Table1.csv")
PATH_GMPI_T2  = Path("Datasets/2024_gMPI_Table1and2 - Table2.csv")
YEAR_MIN, YEAR_MAX = 2000, 2025

def clean_country(val) -> str:
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()

def extract_year(text: str) -> int | None:
    m = re.search(r'(19|20)\d{2}', str(text))
    return int(m.group()) if m else None

def parse_number(val):
    if pd.isna(val):
        return None
    m = re.search(r'[-+]?\d+(?:[.,]\d+)?', str(val).replace(',', '.'))
    return float(m.group()) if m else None

def scale10_to_int(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NA
    return int(round(x * 10))

def py_val(v):
    """converte numpy -> python básico / None"""
    if v is pd.NA or pd.isna(v):
        return None
    if isinstance(v, (np.generic,)):
        return v.item()
    return v

with engine.begin() as conn:
    country_map = (
        pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country"', conn)
          .assign(country_key=lambda d: d["Name"].apply(clean_country))
    )
    year_map = pd.read_sql('SELECT "ID_year", "year" FROM public."Year"', conn)

idh = (
    pd.read_csv(PATH_IDH)
      .rename(columns={"Entity": "country",
                       "Year": "year",
                       "Human Development Index": "idh"})
)
idh = idh[idh["year"].between(YEAR_MIN, YEAR_MAX)].copy()
idh["country_key"] = idh["country"].apply(clean_country)
idh["idh"] = (idh["idh"] * 1000).round().astype("Int64")

t1 = (
    pd.read_csv(PATH_GMPI_T1, skiprows=4)
      .dropna(subset=["Country"])
      .rename(columns={
          "Country": "country",
          "Year and survey": "year_survey",
          "Health": "health",
          "Standard of living": "standard_living"})
      [["country", "year_survey", "health", "standard_living"]]
)
t1["year"] = t1["year_survey"].apply(extract_year)
t1["country_key"] = t1["country"].apply(clean_country)
t1["health"]          = t1["health"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t1["standard_living"] = t1["standard_living"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t1 = t1.dropna(subset=["year"])

t2 = (
    pd.read_csv(PATH_GMPI_T2, skiprows=4)
      .dropna(subset=["Country"])
      .rename(columns={
          "Country": "country",
          "Year and survey": "year_survey",
          "Electricity ": "electricity",
          "Sanitation ": "sanitation"})
      [["country", "year_survey", "electricity", "sanitation"]]
)
t2["year"] = t2["year_survey"].apply(extract_year)
t2["country_key"] = t2["country"].apply(clean_country)
t2["electricity"] = t2["electricity"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t2["sanitation"]  = t2["sanitation"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t2 = t2.dropna(subset=["year"])


indicators = (
    pd.merge(t1, t2[["country_key", "year", "electricity", "sanitation"]],
             on=["country_key", "year"], how="outer")
)

df = pd.merge(idh, indicators,
              on=["country_key", "year"], how="left")

df = (df
      .merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
      .merge(year_map, on="year", how="inner")
)

df_final = (df[["ID_Country", "ID_year",
                "idh", "electricity", "sanitation",
                "health", "standard_living"]]
            .drop_duplicates(subset=["ID_Country", "ID_year"])
)

df_final = df_final.dropna(subset=["idh"]).reset_index(drop=True)


update_sql = text("""
    UPDATE "Country_Year"
       SET "IDH_ID" = :idh_id
     WHERE "Country_ID_Country" = :country_id
       AND "Year_ID_year"    = :year_id
       AND ("IDH_ID" IS NULL OR "IDH_ID" <> :idh_id)
""")

with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        db_cols = []
        params  = {}

        db_cols.append('"IDH"')
        params["idh"] = py_val(row.idh)

        optional = {
            "electricity"    : '"Electricity"',
            "sanitation"     : '"Sanitation"',
            "health"         : '"Health"',
            "standard_living": '"Standard_Living"',
        }
        for attr, db_col in optional.items():
            val = py_val(getattr(row, attr))
            if val is not None:
                db_cols.append(db_col)
                params[attr] = val

        col_list     = ", ".join(db_cols)
        placeholders = ", ".join(f":{p}" for p in params)

        insert_sql = text(f"""
            INSERT INTO "IDH" ({col_list})
            VALUES ({placeholders})
            RETURNING "ID_IDH"
        """)
        new_idh_id = conn.execute(insert_sql, params).scalar_one()

        conn.execute(update_sql, {
            "idh_id"    : new_idh_id,
            "country_id": int(row.ID_Country),
            "year_id"   : int(row.ID_year),
        })
print(f"{len(df_final)} linhas gravadas e vinculadas a Country_Year ✔️")
