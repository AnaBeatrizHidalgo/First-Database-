"""
Script mestre para criar e popular todo o banco de dados (versão simples, SEM rollback global).

Fluxo completo:
1. Cria todas as tabelas a partir de **model_db.sql**.
2. Popula a tabela **Year** (2000‑ano atual).
3. Executa os scripts independentes em ordem lógica:
   - populate_country.py
   - populate_sector.py
   - populate_sector_year.py
4. Popula a tabela **Power Source**.
5. Executa **populate_country_power_source.py**.
6. Popula a tabela **Country_Year** com todas as combinações país-ano.
7. Executa os scripts de indicadores:
   - populate_environmental.py
   - populate_IDH.py
   - populate_gdp.py
   - populate_power_consumed.py
   - fill_population.py

Requisitos:
- Variável de ambiente `DB_URL` (SQLAlchemy) configurada.
- Todos os arquivos *populate_*.py e `model_db.sql` no mesmo diretório do script ou adequar `ROOT`.

Uso:
    python run_all.py
"""
from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    sys.exit("⚠️  Variável de ambiente DB_URL não definida!")

auth_echo = os.getenv("DB_ECHO", "False").lower() in {"1", "true", "yes"}
engine = create_engine(DB_URL, echo=auth_echo)

ROOT: Path = Path(__file__).resolve().parent

def run_sql_file(path: Path) -> None:
    """Executa todas as instruções de um arquivo .sql (separadas por ;)"""
    sql_text = path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    print(f"✅ Executado SQL de {path.name} ({len(statements)} statements)")


def run_sql(statements: str | list[str]) -> None:
    """Executa uma ou mais instruções SQL passadas como string ou lista."""
    if isinstance(statements, str):
        statements = [statements]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def run_script(py_file: str | Path) -> None:
    """Executa um script Python via subprocess, usando o mesmo intérprete."""
    py_path = (ROOT / py_file) if not Path(py_file).is_absolute() else Path(py_file)
    if not py_path.exists():
        sys.exit(f"❌ Script não encontrado: {py_file}")
    print(f"\n🚀  Executando {py_path.name} …")
    subprocess.run([sys.executable, str(py_path)], check=True)

print("\n=== 1/9 – Criando o schema do banco ===")
MODEL_SQL = ROOT / "../Modelos/model_db.sql"
if not MODEL_SQL.exists():
    sys.exit("Arquivo model_db.sql não encontrado!")
run_sql_file(MODEL_SQL)

print("\n=== 2/9 – Populando tabela Year ===")
current_year = date.today().year
sql_year = f"""
INSERT INTO public."Year"(year)
SELECT y FROM generate_series(2000, {current_year}) y
ON CONFLICT DO NOTHING;"""
run_sql(sql_year)
print("✅ Year pronta!")

print("\n=== 3/9 – Scripts de país e setor ===")
for script in ("populate_country.py", "populate_sector.py", "populate_sector_year.py"):
    run_script(script)

print("\n=== 4/9 – Populando tabela Power Source ===")
sql_power_source = """
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
ON CONFLICT DO NOTHING;"""
run_sql(sql_power_source)
print("✅ Power Source pronta!")

print("\n=== 5/9 – Ligando Country x Power Source ===")
run_script("populate_country_power_source.py")

print("\n=== 6/9 – Populando tabela Country_Year ===")
sql_country_year = f"""
INSERT INTO public."Country_Year" ("Country_ID_Country", "Year_ID_year")
SELECT c."ID_Country", y."ID_year"
FROM   public."Country" c
CROSS  JOIN public."Year" y
WHERE  y.year BETWEEN 2000 AND {current_year}
ON CONFLICT DO NOTHING;"""
run_sql(sql_country_year)
print("✅ Country_Year pronta!")

print("\n=== 7/9 – Indicadores (ambiente, IDH, GDP, energia, população) ===")
for script in [
    "populate_environmental.py",
    "populate_IDH.py",
    "populate_gdp.py",
    "populate_power_consumed.py",
    "fill_population.py",
]:
    run_script(script)

print("\n🎉 Banco de dados criado e populado com sucesso! 🎉")
