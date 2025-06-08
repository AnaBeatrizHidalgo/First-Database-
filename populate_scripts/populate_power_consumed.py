import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_PWR = Path("Datasets/PowerConsumid.csv")
YEAR_MIN, YEAR_MAX = 2000, 2025


# ──────────────────────────────
# Funções auxiliares
# ──────────────────────────────
def clean_country(val: str) -> str:
    """Normaliza o nome do país para facilitar o merge."""
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()


def parse_number(val):
    """Extrai o primeiro número (com . ou ,) da célula."""
    if pd.isna(val):
        return None
    m = re.search(r'[-+]?\d+(?:[.,]\d+)?', str(val).replace(',', '.'))
    return float(m.group()) if m else None


def py_val(v):
    """Converte tipos numpy/NA para tipos Python/None."""
    if v is pd.NA or pd.isna(v):
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v


# ──────────────────────────────
# Mapeamento de países (continua igual)
# ──────────────────────────────
with engine.begin() as conn:
    country_map = (
        pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country"', conn)
          .assign(country_key=lambda d: d["Name"].apply(clean_country))
    )

# ──────────────────────────────
# Carrega e limpa o dataset
# ──────────────────────────────
pwr = (
    pd.read_csv(PATH_PWR)
      .rename(columns={
          "Country" : "country",
          "Year"    : "year",
          "Energy imports, net (% of energy use)" :
              "power_import",
          "Renewable energy consumption (% of total final energy consumption)" :
              "renewable",
          "GWH" : "gwh"
      })
)

pwr = pwr[pwr["year"].between(YEAR_MIN, YEAR_MAX)].copy()
pwr["country_key"]   = pwr["country"].apply(clean_country)
pwr["gwh"]           = pwr["gwh"].apply(parse_number)
pwr["power_import"]  = pwr["power_import"].apply(parse_number)
pwr["renewable"]     = pwr["renewable"].apply(parse_number)

# ──────────────────────────────
# Junta com ID do país
# ──────────────────────────────
df = (
    pwr.merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
)

df_final = (
    df[["ID_Country", "year", "gwh", "power_import", "renewable"]]
      .drop_duplicates(subset=["ID_Country", "year"])
      .reset_index(drop=True)
)

# ──────────────────────────────
# UPSERT na nova tabela Power Consumed
# ──────────────────────────────
with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        # Campos dinâmicos (só manda o que não é None)
        col_map = {
            '"GWH"': row.gwh,
            '"PowerImport"': row.power_import,
            '"Renewable_Energy"': row.renewable,
        }
        # Remove None
        col_map = {k: py_val(v) for k, v in col_map.items() if v is not None}

        if not col_map:          # linha vazia – pula
            continue

        # Lista de colunas/valores para INSERT
        cols_extra      = ", ".join(col_map.keys())
        placeholders    = ", ".join(f":{k.strip('\"').lower()}" for k in col_map)
        params          = {k.strip('\"').lower(): v for k, v in col_map.items()}

        # Campos obrigatórios
        params.update({"country_id": int(row.ID_Country),
                       "yr":        int(row.year)})

        insert_sql = text(f"""
            INSERT INTO "Power Consumed"
                ("Country_ID", "Year", {cols_extra})
            VALUES
                (:country_id, :yr, {placeholders})
            ON CONFLICT ("Country_ID", "Year") DO UPDATE
            SET {', '.join(f'{c}=COALESCE(EXCLUDED.{c}, "Power Consumed".{c})' for c in col_map.keys())}
        """)
        conn.execute(insert_sql, params)

print(f"{len(df_final)} registros processados em 'Power Consumed'")
