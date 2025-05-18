#!/usr/bin/env python3
"""
Popula a tabela Country_Power Source (v2)
----------------------------------------
Fonte de dados: PowerGenerationEmission.csv

Campos inseridos:
  • Power_Generation   (numeric 12,2)
  • CO2_Emission       (numeric 12,2)

Requisitos:
  pip install pandas sqlalchemy psycopg2-binary python-dotenv
"""
from pathlib import Path
import os, pandas as pd, unicodedata, logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv



CSV_PATH = Path("Datasets/PowerGenerationEmission.csv")  # 
YEAR_MIN, YEAR_MAX = 2000, 2024

load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("💥  Defina DB_URL no ambiente ou no .env")
engine = create_engine(DB_URL, pool_pre_ping=True)

with engine.begin() as conn:
    country_df = pd.read_sql('SELECT "ID_Country", "Name" FROM "Country";', conn)
    power_df   = pd.read_sql('SELECT "ID_Power", "Name" FROM "Power Source";', conn)
    year_df    = pd.read_sql(
        f'SELECT "ID_year", "year" FROM "Year" WHERE "year" BETWEEN {YEAR_MIN} AND {YEAR_MAX};',
        conn)

country_map = dict(zip(country_df["Name"], country_df["ID_Country"]))
year_map    = dict(zip(year_df["year"],  year_df["ID_year"]))

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

logging.info("Lendo %s…", CSV_PATH)
raw = pd.read_csv(CSV_PATH)

# Mantém apenas 2000-2024
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
        "Area":     "Country",
        "Variable": "Power_Source",
        "Value_gen":"Power_Generation",
        "Value_em": "CO2_Emission"
    })
)

def normalize(txt):
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode().strip()

df["Country_ID_Country"]    = df["Country"].map(country_map)
df["Power Source_ID_Power"] = df["Power_Source"].map(power_map)
df["Year_ID_year"]          = df["Year"].map(year_map)

df = df.dropna(subset=[
    "Country_ID_Country",
    "Power Source_ID_Power",
    "Year_ID_year"
])

for col in ["Power_Generation", "CO2_Emission"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

df_cp = (
    df[[
        "Country_ID_Country",
        "Power Source_ID_Power",
        "Year_ID_year",
        "Power_Generation",
        "CO2_Emission"
    ]]
    .dropna(subset=["Power_Generation", "CO2_Emission"], how="all")  # linhas vazias
    .drop_duplicates()
)

logging.info("Linhas prontas: %d", len(df_cp))

with engine.begin() as conn:
    # tabela temporária
    df_cp.to_sql("tmp_cp", conn, if_exists="replace", index=False)

    insert_sql = """
    INSERT INTO "Country_Power Source"
        ("Country_ID_Country",
         "Power Source_ID_Power",
         "Year_ID_year",
         "Power_Generation",
         "CO2_Emission")
    SELECT  "Country_ID_Country",
            "Power Source_ID_Power",
            "Year_ID_year",
            "Power_Generation",
            "CO2_Emission"
    FROM    tmp_cp
    ON CONFLICT DO NOTHING;
    DROP TABLE tmp_cp;
    """
    conn.execute(text(insert_sql))

print('✅  Tabela "Country_Power Source" populada com sucesso! 🚀')
