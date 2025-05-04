import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ────────────────────────────────
# 1. Config
# ────────────────────────────────
load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_POP = Path("./human-development-index-vs-gdp-per-capita.csv")
YEAR_MIN, YEAR_MAX = 1960, 2025            # população costuma ter histórico longo

# ────────────────────────────────
# 2. Helpers
# ────────────────────────────────
def clean_country(val) -> str:
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()

def py_int(v):
    if pd.isna(v):
        return None
    return int(v)

# ────────────────────────────────
# 3. Países / anos no banco
# ────────────────────────────────
with engine.begin() as conn:
    country_map = (
        pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country"', conn)
          .assign(country_key=lambda d: d["Name"].apply(clean_country))
    )
    year_map = pd.read_sql('SELECT "ID_year", "year" FROM public."Year"', conn)

# ────────────────────────────────
# 4. Carrega população
# ────────────────────────────────
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

# ────────────────────────────────
# 5. Junta com IDs já existentes
# ────────────────────────────────
df = (pop
      .merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
      .merge(year_map, on="year", how="inner")
)

df_final = (df[["ID_Country", "ID_year", "population"]]
            .dropna(subset=["population"])
            .drop_duplicates(subset=["ID_Country", "ID_year"])
            .reset_index(drop=True)
)

# ────────────────────────────────
# 6. Atualiza apenas onde faz sentido
# ────────────────────────────────
update_sql = text("""
    UPDATE "Country_Year"
       SET "Population" = :pop
     WHERE "Country_ID_Country" = :country_id
       AND "Year_ID_year"       = :year_id
       AND ("Population" IS NULL OR "Population" <> :pop)
""")

with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        conn.execute(update_sql, {
            "pop"       : int(row.population),
            "country_id": int(row.ID_Country),
            "year_id"   : int(row.ID_year),
        })

print(f"{len(df_final)} populações gravadas/atualizadas na Country_Year ✔️")
