import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Conectar no banco
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Ler a aba correta
edgar = pd.read_excel("EDGAR_2024_GHG_booklet_2024_fossilCO2only.xlsx", sheet_name="fossil_CO2_by_sector_country_su")

# Agora pegar a coluna 'Sector' diretamente
df_setor = edgar[['Sector']].dropna().drop_duplicates()
df_setor = df_setor.rename(columns={'Sector': 'Name'})

# Limitar nome do setor para 100 caracteres (só por segurança)
df_setor['Name'] = df_setor['Name'].str.slice(0, 100)

# Inserir no banco
with engine.begin() as conn:
    df_setor.to_sql('Setor', conn, if_exists='append', index=False)

print("Tabela 'Setor' populada com sucesso! 🚀")
