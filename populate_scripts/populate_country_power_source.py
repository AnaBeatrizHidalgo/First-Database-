#!/usr/bin/env python3
"""
Popula a tabela Power Source_Country (modelo v3)
------------------------------------------------
Fonte de dados: PowerGenerationEmission.csv
"""

from pathlib import Path
import os, pandas as pd, unicodedata, logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

CSV_PATH = Path("Datasets/PowerGenerationEmission.csv")
YEAR_MIN, YEAR_MAX = 2000, 2024

load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("💥  Defina DB_URL no ambiente ou no .env")
engine = create_engine(DB_URL, pool_pre_ping=True)

# ─────────────────────────────
# Mapeamento de entidades
# ─────────────────────────────
with engine.begin() as conn:
    country_df = pd.read_sql('SELECT "ID_Country", "Name" FROM "Country";', conn)
    power_df   = pd.read_sql('SELECT "ID_Power", "Name" FROM "Power Source";', conn)

country_map = dict(zip(country_df["Name"], country_df["ID_Country"]))

# Mapeia nomes do CSV → nomes oficiais no banco
var_to_power = {
    "Bioenergy":        "Bioenergy",
    "Coal":             "Coal",
    "Gas":              "Gas",
    "Hydro":            "Hydro",
    "Nuclear":          "Nuclear",
    "Other Fossil":     "Oil",  # aproximação
    "Other Renewables": "Other renewables excluding bioenergy",
    "Solar":            "Solar",
    "Wind":             "Wind",
}
power_map = {k: power_df.loc[power_df["Name"] == v, "ID_Power"].squeeze()
             for k, v in var_to_power.items()}

# ─────────────────────────────
# Leitura e processamento do CSV
# ─────────────────────────────
raw = pd.read_csv(CSV_PATH)
raw = raw[raw["Year"].between(YEAR_MIN, YEAR_MAX)].copy()

df_gen = raw[raw["Category"] == "Electricity generation"]
df_em  = raw[raw["Category"] == "Power sector emissions"]

df = (
    df_gen.merge(
        df_em,
        on=["Area", "Year", "Variable"],
        how="outer",
        suffixes=("_gen", "_em")
    )
    .rename(columns={
        "Area":     "country",
        "Variable": "power_source",
        "Value_gen": "power_generation",
        "Value_em":  "co2_emission"
    })
)

# Normalize
def normalize(txt):
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode().strip()

df["country_id"]  = df["country"].map(country_map)
df["power_id"]    = df["power_source"].map(power_map)
df["year"]        = df["Year"].astype(int)

df = df.dropna(subset=["country_id", "power_id", "year"])

for col in ["power_generation", "co2_emission"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

df_cp = (
    df[["country_id", "power_id", "year", "power_generation", "co2_emission"]]
    .dropna(subset=["power_generation", "co2_emission"], how="all")
    .drop_duplicates()
)

# ─────────────────────────────
# UPSERT direto na Power Source_Country
# ─────────────────────────────
with engine.begin() as conn:
    for row in df_cp.itertuples(index=False):
        col_map = {}
        if row.power_generation is not None:
            col_map['"Power_Generation"'] = row.power_generation
        if row.co2_emission is not None:
            col_map['"CO2_Emission"'] = row.co2_emission

        if not col_map:
            continue

        col_names    = ", ".join(col_map.keys())
        placeholders = ", ".join(f":{k.strip('\"').lower()}" for k in col_map)
        params       = {k.strip('\"').lower(): v for k, v in col_map.items()}

        # Campos obrigatórios
        params.update({
            "country_id": int(row.country_id),
            "power_id":   int(row.power_id),
            "yr":         int(row.year)
        })

        insert_sql = text(f"""
            INSERT INTO "Power Source_Country"
                ("Country_ID_Country", "Power Source_ID_Power", "Year", {col_names})
            VALUES
                (:country_id, :power_id, :yr, {placeholders})
            ON CONFLICT ("Country_ID_Country", "Power Source_ID_Power", "Year") DO UPDATE
            SET {', '.join(f'{k} = COALESCE(EXCLUDED.{k}, "Power Source_Country".{k})' for k in col_map)}
        """)
        conn.execute(insert_sql, params)

print(f"{len(df_cp)} registros inseridos/atualizados em 'Power Source_Country'")
