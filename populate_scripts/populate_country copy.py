import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Conectar no banco
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Leitura do dataset de países
countries = pd.read_csv("WDICountry.csv")

# Selecionar colunas relevantes
# Selecionar apenas Name e Region
df_country = countries[['Table Name']].rename(columns={
    'Table Name': 'Name',
})

# Filtrar apenas linhas válidas

# Tratar nomes muito grandes (se quiser garantir)
df_country['Name'] = df_country['Name'].str.slice(0, 100)

# Inserir no banco
with engine.begin() as conn:
    df_country.to_sql('Country', conn, if_exists='append', index=False)

print("Tabela 'Country' populada com sucesso! 🚀")
