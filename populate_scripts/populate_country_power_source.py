"""
Popula a tabela "Country_Power Source" com os dados de geração elétrica
contidos em electricity-prod-source-stacked.csv, usando o mesmo “modelo”
(pandas + SQLAlchemy + dotenv) que você adotou para a tabela Country.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ──────────────────────────────────────────────────────────────
# 1. CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────
load_dotenv()                       # carrega .env na raiz do projeto
DB_URL = os.getenv("DB_URL")        # ex.: postgres+psycopg2://user:pass@host:5432/db
engine = create_engine(DB_URL)

CSV_PATH = "electricity-prod-source-stacked.csv"

# ──────────────────────────────────────────────────────────────
# 2. CARREGA MAPAS (Country e Power Source) DIRECTAMENTE DO BANCO
# ──────────────────────────────────────────────────────────────
with engine.begin() as conn:
    country_map = pd.read_sql('SELECT "ID_Country", "Name" FROM "Country"', conn)
    power_map   = pd.read_sql('SELECT "ID_Power", "Name" FROM "Power Source"', conn)

# converte em dicionários para mapeamento rápido
country_dict = dict(zip(country_map["Name"], country_map["ID_Country"]))
power_dict   = dict(zip(power_map["Name"], power_map["ID_Power"]))

# ──────────────────────────────────────────────────────────────
# 3. LÊ O CSV E TRANSFORMA PARA FORMATO LONG
# ──────────────────────────────────────────────────────────────
raw = pd.read_csv(CSV_PATH)

# colunas “largas” → rótulos curtos
rename_cols = {
    col: col.split("Electricity from ")[1].split(" -")[0].title().replace("Bioenergy", "Biomass")
    for col in raw.columns if col.startswith("Electricity from")
}
raw = raw.rename(columns=rename_cols)

# converte para formato long (uma linha por (país, ano, fonte))
df = (
    raw.melt(id_vars=["Entity", "Code", "Year"],
             value_vars=list(rename_cols.values()),
             var_name="Power_Source",
             value_name="Power_Generation")
       .dropna(subset=["Power_Generation"])
)

# ──────────────────────────────────────────────────────────────
# 4. MAPEIA IDS E LIMPA DADOS
# ──────────────────────────────────────────────────────────────
df["Country_ID_Country"]   = df["Entity"].map(country_dict)
df["Power Source_ID_Power"] = df["Power_Source"].map(power_dict)
df["Year_ID_year"]         = df["Year"].astype(int)

# remove linhas cujo país ou fonte não exista no banco
df = df.dropna(subset=["Country_ID_Country", "Power Source_ID_Power"])

# seleciona e ordena colunas no mesmo layout da tabela
df_cp = df[[
    "Country_ID_Country",
    "Power Source_ID_Power",
    "Year_ID_year",
    "Power_Generation"
]].copy()

# arredonda geração para 2 casas (opcional)
df_cp["Power_Generation"] = df_cp["Power_Generation"].astype(float).round(2)

# remove duplicatas para evitar conflito na PK composta
df_cp = df_cp.drop_duplicates()

# ──────────────────────────────────────────────────────────────
# 5. INSERE NO BANCO
# ──────────────────────────────────────────────────────────────
with engine.begin() as conn:
    # cria staging temporária (opcional mas evita problema de conflito)
    df_cp.to_sql("tmp_cp", conn, if_exists="replace", index=False)

    # faz upsert simples usando ON CONFLICT DO NOTHING
    insert_sql = """
    INSERT INTO "Country_Power Source"
        ("Country_ID_Country",
         "Power Source_ID_Power",
         "Year_ID_year",
         "Power_Generation")
    SELECT  "Country_ID_Country",
            "Power Source_ID_Power",
            "Year_ID_year",
            "Power_Generation"
    FROM    tmp_cp
    ON CONFLICT DO NOTHING;
    DROP TABLE tmp_cp;
    """
    conn.execute(text(insert_sql))

print('Tabela "Country_Power Source" populada com sucesso! ⚡🚀')
