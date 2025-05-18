#!/usr/bin/env python3
"""
Script mestre para criar e popular todo o banco de dados
(v.2 – inclui populate_sector_country_year.py)

Fluxo:
1. Cria todas as tabelas a partir de **model_db.sql**.
2. Popula a dimensão **Year** (2000-ano atual).
3. Executa scripts de dimensões básicas:
   - populate_country.py
   - populate_sector.py
   - populate_sector_year.py
   - populate_sector_country_year.py   ← NOVO!
4. Popula a dimensão **Power Source** e a ponte **Country_Power_Source**.
5. Cria **Country_Year** (todas as combinações).
6. Executa scripts de indicadores:
   - populate_environmental.py
   - populate_IDH.py
   - populate_gdp.py
   - populate_power_consumed.py
   - fill_population.py
"""

from __future__ import annotations
import os, sys, subprocess
from pathlib import Path
from datetime import date

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ------------------------------------------------------------------#
# 0. Conexão
# ------------------------------------------------------------------#
load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    sys.exit("⚠️  Variável de ambiente DB_URL não definida!")

engine = create_engine(DB_URL, echo=os.getenv("DB_ECHO", "false").lower() in {"1", "true", "yes"})
ROOT: Path = Path(__file__).resolve().parent  # pasta populate_scripts

# ------------------------------------------------------------------#
# Helpers
# ------------------------------------------------------------------#
def run_sql_file(path: Path) -> None:
    """Roda tudo que encontrar num .sql, separando por ';'."""
    sql_text = path.read_text(encoding="utf-8")
    stmts = [s.strip() for s in sql_text.split(";") if s.strip()]
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))
    print(f"✅ {path.name}: {len(stmts)} statements executados")

def run_sql(sql: str | list[str]) -> None:
    if isinstance(sql, str):
        sql = [sql]
    with engine.begin() as conn:
        for stmt in sql:
            conn.execute(text(stmt))

def run_script(py_file: str | Path) -> None:
    """Executa outro .py com o mesmo interpretador."""
    path = (ROOT / py_file) if not Path(py_file).is_absolute() else Path(py_file)
    if not path.exists():
        sys.exit(f"❌ Script não encontrado: {py_file}")
    print(f"\n🚀  {path.name}")
    subprocess.run([sys.executable, str(path)], check=True)

# ------------------------------------------------------------------#
# 1. Criação das tabelas
# ------------------------------------------------------------------#
MODEL_SQL = ROOT / "../Modelos/model_db.sql"
if not MODEL_SQL.exists():
    sys.exit("❌ model_db.sql não encontrado!")
run_sql_file(MODEL_SQL)

# ------------------------------------------------------------------#
# 2. Dimensão YEAR (2000-ANO ATUAL)
# ------------------------------------------------------------------#
current_year = date.today().year
run_sql(f"""
INSERT INTO public."Year"(year)
SELECT y FROM generate_series(2000, {current_year}) y
ON CONFLICT DO NOTHING;
""")

# ------------------------------------------------------------------#
# 3. Scripts de dimensões básicas
# ------------------------------------------------------------------#
for script in (
    "populate_country.py",
    "populate_sector.py",
    "populate_sector_year.py",
    "populate_sector_country_year.py",   # ← novo passo
):
    run_script(script)

# ------------------------------------------------------------------#
# 4. Power Source + Country_Power_Source
# ------------------------------------------------------------------#
run_sql("""
INSERT INTO public."Power Source" ("Name", "Renewable") VALUES
 ('Other renewables excluding bioenergy', true),
 ('Bioenergy', true),
 ('Solar', true),
 ('Wind', true),
 ('Hydro', true),
 ('Nuclear', false),
 ('Oil', false),
 ('Gas', false),
 ('Coal', false)
ON CONFLICT DO NOTHING;
""")

run_script("populate_country_power_source.py")

# ------------------------------------------------------------------#
# 5. Country_Year (todas as combinações 2000-atual)
# ------------------------------------------------------------------#
run_sql(f"""
INSERT INTO public."Country_Year" ("Country_ID_Country", "Year_ID_year")
SELECT c."ID_Country", y."ID_year"
FROM   public."Country" c
CROSS  JOIN public."Year" y
WHERE  y.year BETWEEN 2000 AND {current_year}
ON CONFLICT DO NOTHING;
""")

# ------------------------------------------------------------------#
# 6. Indicadores
# ------------------------------------------------------------------#
for script in (
    "populate_environmental.py",
    "populate_IDH.py",
    "populate_gdp.py",
    "populate_power_consumed.py",
    "fill_population.py",
):
    run_script(script)

print("\n🎉 Banco de dados criado e populado com sucesso! 🎉")
