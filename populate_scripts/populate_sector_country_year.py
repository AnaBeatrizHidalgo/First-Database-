#!/usr/bin/env python3
"""
Populate public."Sector_Country_Year" a partir do EDGAR
-------------------------------------------------------
Requisitos:
  pip install pandas sqlalchemy psycopg2-binary python-dotenv
"""

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os, unicodedata, logging
from pathlib import Path



load_dotenv()                          # lê .env
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("💥  Defina DB_URL no ambiente ou no .env")

engine = create_engine(DB_URL, pool_pre_ping=True)

DATAFILE = Path("./Datasets/EDGAR_2024_GHG_booklet_2024_fossilCO2only.xlsx")
SHEET    = "fossil_CO2_by_sector_country_su"

df = pd.read_excel(DATAFILE, sheet_name=SHEET)

def clean_col(col):
    if isinstance(col, str):
        return col.strip()
    if isinstance(col, float) and col.is_integer():
        return int(col)
    return col
df.columns = df.columns.map(clean_col)


AGGREGATES = {"GLOBAL TOTAL", "OECD TOTAL", "NON-OECD TOTAL", "EU27"}
df = df[~df["EDGAR Country Code"].str.upper().isin(AGGREGATES)].copy()

# Helper de normalização (remove acentos/caixa)
def normalize(val) -> str:
    """
    Converte qualquer valor em string ASCII MAIÚSCULA.
    Se for NaN/None, devolve ''.
    """
    if pd.isna(val):
        return ""
    if not isinstance(val, str):
        val = str(val)
    return (
        unicodedata.normalize("NFKD", val)
        .encode("ascii", "ignore")
        .decode()
        .upper()
        .strip()
    )

with engine.begin() as conn:
    countries = pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country";', conn)
    country_id_map = dict(zip(countries["Name"].apply(normalize), countries["ID_Country"]))

    sectors = pd.read_sql('SELECT "ID_Sector", "Name" FROM public."Sector";', conn)
    sector_id_map = dict(zip(sectors["Name"].apply(normalize), sectors["ID_Sector"]))

    years_df = pd.read_sql(
        'SELECT "ID_year", "year" FROM public."Year" WHERE "year" BETWEEN 2000 AND 2023;',
        conn)
    year_id_map = dict(zip(years_df["year"].astype(int), years_df["ID_year"]))

def is_year_col(col):
    try:
        yr = int(float(col))
        return 2000 <= yr <= 2023
    except (TypeError, ValueError):
        return False

available_years = [
    int(float(c))
    for c in df.columns
    if is_year_col(c) and int(float(c)) in year_id_map
]

records, missing_countries, missing_sectors = [], set(), set()

for _, row in df.iterrows():
    sector_key  = normalize(row["Sector"])
    country_key = normalize(row["Country"])

    sector_id  = sector_id_map.get(sector_key)
    country_id = country_id_map.get(country_key)

    if not sector_id:            
        missing_sectors.add(row["Sector"])
        continue
    if not country_id:
        missing_countries.add(row["Country"])
        continue

    for year in available_years:
        emission = row.get(year)
        if pd.notnull(emission):
            records.append(
                {
                    "Sector_ID_Sector":   int(sector_id),
                    "Country_ID_Country": int(country_id),
                    "Year_ID_year":       int(year_id_map[year]),
                    "CO2_Emission":       round(float(emission), 2),
                }
            )

if records:
    df_final = pd.DataFrame(records)
    with engine.begin() as conn:
        df_final.to_sql(
            "Sector_Country_Year",
            conn,
            schema="public",
            if_exists="append",
            index=False,
            method="multi",           # envia lotes grandes
        )
    logging.info("✅  Inseridas %d linhas!", len(df_final))
else:
    logging.warning("Nada a inserir 🤔")
