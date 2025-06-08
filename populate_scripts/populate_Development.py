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

indicators = pd.merge(
    t1,
    t2[["country_key", "year", "electricity", "sanitation"]],
    on=["country_key", "year"], how="outer"
)

df = pd.merge(idh, indicators, on=["country_key", "year"], how="left")
df = pd.merge(df, country_map[["ID_Country", "country_key"]], on="country_key", how="inner")

df_final = (
    df[["ID_Country", "year", "idh", "electricity", "sanitation", "health", "standard_living"]]
      .dropna(subset=["idh"])
      .drop_duplicates(subset=["ID_Country", "year"])
      .reset_index(drop=True)
)

with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        col_map = {
            '"IDH"'           : row.idh,
            '"Electricity"'   : row.electricity,
            '"Sanitation"'    : row.sanitation,
            '"Health"'        : row.health,
            '"Standard_Living"': row.standard_living,
        }

        # Remove colunas com valor nulo
        col_map = {k: py_val(v) for k, v in col_map.items() if v is not None}
        if not col_map:
            continue

        col_list     = ", ".join(col_map.keys())
        placeholders = ", ".join(f":{k.strip('\"').lower()}" for k in col_map)
        params       = {k.strip('\"').lower(): v for k, v in col_map.items()}

        # obrigatórios
        params.update({
            "country_id": int(row.ID_Country),
            "ano":        int(row.year),
        })

        insert_sql = text(f"""
            INSERT INTO "Development" 
                ("Country_ID", "Ano", {col_list})
            VALUES 
                (:country_id, :ano, {placeholders})
            ON CONFLICT ("Country_ID", "Ano") DO UPDATE
            SET {', '.join(f'{c} = COALESCE(EXCLUDED.{c}, "Development".{c})' for c in col_map.keys())}
        """)
        conn.execute(insert_sql, params)

print(f"{len(df_final)} registros inseridos/atualizados em 'Development'")
