import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_POP = Path("Datasets/human-development-index-vs-gdp-per-capita.csv")
YEAR_MIN, YEAR_MAX = 2000, 2025           

def clean_country(val) -> str:
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()

def py_int(v):
    if pd.isna(v):
        return None
    return int(v)

# ─────────────────────────────────
# Mapeamento dos países
# ─────────────────────────────────
with engine.begin() as conn:
    country_map = (
        pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country"', conn)
          .assign(country_key=lambda d: d["Name"].apply(clean_country))
    )

# ─────────────────────────────────
# Leitura e processamento do CSV
# ─────────────────────────────────
pop = (
    pd.read_csv(PATH_POP)
      .rename(columns={
          "Entity"     : "country",
          "Year"       : "year",
          "Population (historical)" : "population"
      })
      [["country", "year", "population"]]
)
pop = pop[pop["year"].between(YEAR_MIN, YEAR_MAX)].copy()
pop["country_key"] = pop["country"].apply(clean_country)
pop["population"]  = pop["population"].apply(py_int)

# ─────────────────────────────────
# Merge com ID do país
# ─────────────────────────────────
df = (
    pop
    .merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
)

df_final = (
    df[["ID_Country", "year", "population"]]
      .dropna(subset=["population"])
      .drop_duplicates(subset=["ID_Country", "year"])
      .reset_index(drop=True)
)

# ─────────────────────────────────
# UPSERT direto em Demography
# ─────────────────────────────────
with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        insert_sql = text("""
            INSERT INTO "Demography"
                ("Country_ID", "Year", "Population")
            VALUES
                (:country_id, :year, :population)
            ON CONFLICT ("Country_ID", "Year") DO UPDATE
            SET "Population" = EXCLUDED."Population"
        """)
        conn.execute(insert_sql, {
            "country_id": int(row.ID_Country),
            "year":       int(row.year),
            "population": int(row.population),
        })

print(f"{len(df_final)} registros inseridos/atualizados em 'Demography'")
