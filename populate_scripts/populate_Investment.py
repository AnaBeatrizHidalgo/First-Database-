import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_GDP = Path("Datasets/country_year_gdp_energy_health.csv")  
YEAR_MIN, YEAR_MAX = 1960, 2025

def clean_country(val) -> str:
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()

def parse_float(val):
    if pd.isna(val):
        return None
    try:
        return float(val)
    except ValueError:
        return None

def py_val(v):
    if v is pd.NA or pd.isna(v):
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v

# ────────────────
# Mapeamento de países
# ────────────────
with engine.begin() as conn:
    country_map = (
        pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country"', conn)
          .assign(country_key=lambda d: d["Name"].apply(clean_country))
    )

# ────────────────
# Leitura e limpeza do dataset
# ────────────────
df_src = (
    pd.read_csv(PATH_GDP)
      .rename(columns={
          "country_name": "country",
          "country_code": "code"
      })
)

df_src["country_key"] = df_src["country"].apply(clean_country)
df_src["gdp"]                = df_src["gdp"].apply(parse_float)
df_src["investment_energy"]  = df_src["investment_energy"].apply(parse_float)
df_src["health_expenditure"] = df_src["health_expenditure"].apply(parse_float)

df_src = df_src[df_src["year"].between(YEAR_MIN, YEAR_MAX)]

df = (
    df_src
    .merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
)

df_final = (
    df[["ID_Country", "year", "gdp", "investment_energy", "health_expenditure"]]
    .dropna(subset=["gdp"])
    .drop_duplicates(subset=["ID_Country", "year"])
    .reset_index(drop=True)
)

# ────────────────
# UPSERT em Investment
# ────────────────
with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        col_map = {
            '"GDP"': py_val(row.gdp),
        }

        optional = {
            "investment_energy": '"Investment_Energy"',
            "health_expenditure": '"Health_Expenditure"',
        }
        for attr, db_col in optional.items():
            val = py_val(getattr(row, attr))
            if val is not None:
                col_map[db_col] = val

        if not col_map:
            continue

        col_names   = ", ".join(col_map.keys())
        placeholders = ", ".join(f":{k.strip('\"').lower()}" for k in col_map)
        params = {k.strip('\"').lower(): v for k, v in col_map.items()}

        # Campos obrigatórios
        params.update({
            "country_id": int(row.ID_Country),
            "yr":         int(row.year),
        })

        insert_sql = text(f"""
            INSERT INTO "Investment"
                ("Country_ID", "Year", {col_names})
            VALUES
                (:country_id, :yr, {placeholders})
            ON CONFLICT ("Country_ID", "Year") DO UPDATE
            SET {', '.join(f'{c} = COALESCE(EXCLUDED.{c}, "Investment".{c})' for c in col_map)}
        """)
        conn.execute(insert_sql, params)

print(f"{len(df_final)} registros inseridos/atualizados na tabela 'Investment'")
