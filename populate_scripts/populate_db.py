#!/usr/bin/env python3
"""
Script mestre para criar e popular todo o banco de dados  (versão 3)
--------------------------------------------------------------------
Fluxo:
1. Cria as tabelas a partir de Modelos/model_db.sql.
2. Executa scripts de dimensões básicas:
      • populate_country.py
      • populate_sector.py
3. Insere os registros‐semente de Power Source.
4. Executa scripts de fatos / pontes:
      • populate_sector_country.py
      • populate_country_power_source.py
5. Executa scripts de indicadores:
      • populate_environmental.py
      • populate_Development.py
      • populate_Investment.py
      • populate_power_consumed.py
      • populate_demography.py
"""

from __future__ import annotations
import os, sys, subprocess
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ───────────────────────────────────────────────────────────────
# 0. Conexão
# ───────────────────────────────────────────────────────────────
load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    sys.exit("Variável de ambiente DB_URL não definida!")

engine = create_engine(
    DB_URL,
    echo=os.getenv("DB_ECHO", "false").lower() in {"1", "true", "yes"},
    pool_pre_ping=True,
)
ROOT: Path = Path(__file__).resolve().parent  # pasta populate_scripts

# ───────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────
def run_sql_file(path: Path) -> None:
    """Executa todas as instruções de um .sql (separadas por ';')."""
    sql_text = path.read_text(encoding="utf-8")
    stmts = [s.strip() for s in sql_text.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in stmts:
            conn.execute(text(stmt))
    print(f"{path.name}: {len(stmts)} statements executados")

def run_sql(sql: str | list[str]) -> None:
    if isinstance(sql, str):
        sql = [sql]
    with engine.begin() as conn:
        for stmt in sql:
            conn.execute(text(stmt))

def run_script(py_file: str | Path) -> None:
    """Executa outro script Python com o mesmo interpretador."""
    path = (ROOT / py_file) if not Path(py_file).is_absolute() else Path(py_file)
    if not path.exists():
        sys.exit(f"Script não encontrado: {py_file}")
    print(f"\n🚀  {path.name}")
    subprocess.run([sys.executable, str(path)], check=True)

# ───────────────────────────────────────────────────────────────
# 1. Criação das tabelas
# ───────────────────────────────────────────────────────────────
MODEL_SQL = ROOT / "../Modelos/modeloFisico.sql"
if not MODEL_SQL.exists():
    sys.exit("modeloFisico.sql não encontrado!")
run_sql_file(MODEL_SQL)

# ───────────────────────────────────────────────────────────────
# 2. Dimensões básicas
# ───────────────────────────────────────────────────────────────
for script in (
    "populate_country.py",
    "populate_sector.py",
):
    run_script(script)

# ───────────────────────────────────────────────────────────────
# 3. Power Source (dimensão fixa) + tabelas‐ponte
# ───────────────────────────────────────────────────────────────
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

run_script("populate_sector_country.py")
run_script("populate_country_power_source.py")

# ───────────────────────────────────────────────────────────────
# 4. Indicadores
# ───────────────────────────────────────────────────────────────
for script in (
    "populate_environmental.py",
    "populate_Development.py",
    "populate_Investment.py",
    "populate_power_consumed.py",
    "populate_demography.py",
):
    run_script(script)

print("\nBanco de dados criado e populado com sucesso!")
