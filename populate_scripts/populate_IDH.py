import re
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from pathlib import Path

# ────────────────────────────────
# 1. Config / caminhos
# ────────────────────────────────
load_dotenv()                                # .env com DB_URL
engine = create_engine(os.getenv("DB_URL"))

PATH_IDH      = Path("./human-development-index-vs-gdp-per-capita.csv")
PATH_GMPI_T1  = Path("./2024_gMPI_Table1and2 - gMPI_Table1.csv")   # Health + Standard of living
PATH_GMPI_T2  = Path("./2024_gMPI_Table1and2 - Table2.csv")   # Electricity + Sanitation (ajuste se diferente)
YEAR_MIN, YEAR_MAX = 2000, 2025

# ────────────────────────────────
# 2. Funções utilitárias
# ────────────────────────────────
def clean_country(name: str) -> str:
    return re.sub(r"\s+", " ", str(name)).strip().casefold()      # lower + trim

def extract_year(text: str) -> int | None:
    """Extrai o primeiro ano (AAAA) que aparecer em '2022/2023 M' → 2022"""
    m = re.search(r"(19|20)\d{2}", str(text))
    return int(m.group()) if m else None

def parse_number(val) -> float | None:
    """
    Converte strings como '24,1', '0,360', ' 12.7 ', '0,010'
    em número float. Qualquer coisa sem dígito vira None.
    """
    if pd.isna(val):
        return None

    s = str(val).strip()
    # Normaliza vírgula→ponto
    s = s.replace(",", ".")

    # Extrai primeiro padrão numérico (ex.: '(% )' devolve None)
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        return None

    try:
        return float(m.group())
    except ValueError:
        return None
    
def scale10_to_int(x):
    """
    Se x é None/NaN → pd.NA
    Caso contrário → int(round(x * 10))
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NA
    return int(round(x * 10))
    
# ────────────────────────────────
# 3. IDH – primeira planilha
# ────────────────────────────────
idh = (
    pd.read_csv(PATH_IDH)
      .rename(columns={"Entity": "country", "Year": "year", "Human Development Index": "idh"})
)
idh["country_key"] = idh["country"].apply(clean_country)
idh = idh[(idh["year"] >= YEAR_MIN) & (idh["year"] <= YEAR_MAX)]
idh["idh"] = (idh["idh"] * 1000).round().astype("Int64")          # inteiro 0-1000

# ────────────────────────────────
# 4. gMPI – Table 1 (health, standard_living)
# ────────────────────────────────
t1 = (
    pd.read_csv(PATH_GMPI_T1, skiprows=4)                          # pula cabeçalhos textuais
      .rename(columns={
          "Country": "country",
          "Year and survey": "year_survey",
          "Health": "health",
          "Standard of living": "standard_living",
      })
      [["country", "year_survey", "health", "standard_living"]]
)
t1["year"]        = t1["year_survey"].apply(extract_year)
t1["country_key"] = t1["country"].apply(clean_country)
t1["health"]           = t1["health"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t1["standard_living"]  = t1["standard_living"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t1 = t1.dropna(subset=["year"])

# ────────────────────────────────
# 5. gMPI – Table 2 (electricity, sanitation)
# ────────────────────────────────
t2 = (
    pd.read_csv(PATH_GMPI_T2, skiprows=4)
      .rename(columns={
          "Country": "country",
          "Year and survey": "year_survey",
          "Electricity": "electricity",
          "Sanitation": "sanitation",
      })
      [["country", "year_survey", "electricity", "sanitation"]]
)
t2["year"]        = t2["year_survey"].apply(extract_year)
t2["country_key"] = t2["country"].apply(clean_country)
t2["electricity"] = t2["electricity"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t2["sanitation"]  = t2["sanitation"].apply(parse_number).apply(scale10_to_int).astype("Int64")
t2 = t2.dropna(subset=["year"])

# ────────────────────────────────
# 6. Merge incremental
# ────────────────────────────────
# junta health/standard + electricity/sanitation
indicators = (
      pd.merge(t1, t2[["country_key","year","electricity","sanitation"]],
               on=["country_key","year"], how="outer")
      [["country_key","year","health","standard_living","electricity","sanitation"]]
)

# cola no IDH
df_final = (
    pd.merge(idh, indicators, on=["country_key","year"], how="left")
      [["country","year","idh","electricity","sanitation","health","standard_living"]]
      .drop_duplicates(subset=["country","year"])
)

# ────────────────────────────────
# 7. Inserção no banco
# ────────────────────────────────
with engine.begin() as conn:
    df_final.to_sql('IDH', conn, if_exists='append', index=False, method='multi')

print(f"{len(df_final)} registros gravados na tabela IDH 🚀")
