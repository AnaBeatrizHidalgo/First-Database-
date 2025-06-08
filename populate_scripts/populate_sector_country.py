#!/usr/bin/env python3
"""
Popula public."Sector_Country" a partir do EDGAR
-----------------------------------------------
Atualizado para o modelo sem tabela Year
"""

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os, unicodedata, logging
from pathlib import Path

load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("💥  Defina DB_URL no ambiente ou no .env")

engine = create_engine(DB_URL, pool_pre_ping=True)

DATAFILE = Path("./Datasets/EDGAR_2024_GHG_booklet_2024_fossilCO2only.xlsx")
SHEET    = "fossil_CO2_by_sector_country_su"

# ────────────────
# Leitura e limpeza do DataFrame
# ────────────────
df = pd.read_excel(DATAFILE, sheet_name=SHEET)

def clean_col(col):
    if isinstance(col, str):
        return col.strip()
    if isinstance(col, float) and col.is_integer():
        return int(col)
    return col
df.columns = df.columns.map(clean_col)

# Remove agregações globais
AGGREGATES = {"GLOBAL TOTAL", "OECD TOTAL", "NON-OECD TOTAL", "EU27"}
df = df[~df["EDGAR Country Code"].str.upper().isin(AGGREGATES)].copy()

# ────────────────
# Normalização de textos
# ────────────────
def normalize(val) -> str:
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

# ────────────────
# Identifica colunas de ano válidas
# ────────────────
def is_year_col(col):
    try:
        yr = int(float(col))
        return 2000 <= yr <= 2023
    except (TypeError, ValueError):
        return False

available_years = [
    int(float(c)) for c in df.columns if is_year_col(c)
]

# ────────────────
# Geração dos registros prontos para inserção
# ────────────────
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
            records.append({
                "Sector_ID_Sector":   int(sector_id),
                "Country_ID_Country": int(country_id),
                "Year":               int(year),
                "CO2_Emission":       round(float(emission), 2),
            })

# ────────────────
# Inserção em lote
# ────────────────
if records:
    df_final = pd.DataFrame(records)
    with engine.begin() as conn:
        df_final.to_sql(
            "Sector_Country",
            conn,
            schema="public",
            if_exists="append",
            index=False,
            method="multi"
        )
    logging.info("✅ Inseridas %d linhas em Sector_Country", len(df_final))
else:
    logging.warning("⚠️  Nada a inserir")

# Dica de debugging se quiser ver faltantes:
if missing_countries:
    print("Países não encontrados:", sorted(missing_countries))
if missing_sectors:
    print("Setores não encontrados:", sorted(missing_sectors))
