import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_ENV = Path("Datasets/CombinandoEnviromental.csv")  
YEAR_MIN, YEAR_MAX = 2000, 2025                  

def clean_country(val) -> str:
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()

def parse_number(val):
    if pd.isna(val):
        return None
    m = re.search(r'[-+]?\d+(?:[.,]\d+)?', str(val).replace(',', '.'))
    return float(m.group()) if m else None

def py_val(v):
    if v is pd.NA or pd.isna(v):
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v

# ─────────────────────────────
# Mapeamento de países
# ─────────────────────────────
with engine.begin() as conn:
    country_map = (
        pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country"', conn)
          .assign(country_key=lambda d: d["Name"].apply(clean_country))
    )

# ─────────────────────────────
# Carregamento e limpeza
# ─────────────────────────────
env = (
    pd.read_csv(PATH_ENV)
      .rename(columns={
          "Country": "country",
          "Year": "year",
          "CO2 Emission": "co2"
      })
)
env = env[env["year"].between(YEAR_MIN, YEAR_MAX)].copy()
env["country_key"] = env["country"].apply(clean_country)
env["co2"]  = env["co2"].apply(parse_number)

df = (
    env.merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
)

df_final = (
    df[["ID_Country", "year", "co2"]]
      .drop_duplicates(subset=["ID_Country", "year"])
      .reset_index(drop=True)
)

# ─────────────────────────────
# UPSERT na Environmental Indicator
# ─────────────────────────────
with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        col_map = {}

        if row.co2 is not None:
            col_map['"CO2_Emision"'] = py_val(row.co2)

        if not col_map:
            continue

        col_names    = ", ".join(col_map.keys())
        placeholders = ", ".join(f":{k.strip('\"').lower()}" for k in col_map)
        params       = {k.strip('\"').lower(): v for k, v in col_map.items()}

        # Campos obrigatórios
        params.update({
            "country_id": int(row.ID_Country),
            "yr":         int(row.year)
        })

        insert_sql = text(f"""
            INSERT INTO "Environmental Indicator"
                ("Country_ID", "Year", {col_names})
            VALUES
                (:country_id, :yr, {placeholders})
            ON CONFLICT ("Country_ID", "Year") DO UPDATE
            SET {', '.join(f'{c} = COALESCE(EXCLUDED.{c}, "Environmental Indicator".{c})' for c in col_map)}
        """)
        conn.execute(insert_sql, params)

print(f"{len(df_final)} registros inseridos/atualizados em 'Environmental Indicator'")
