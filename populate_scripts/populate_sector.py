import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

edgar = pd.read_excel("./Datasets/Dados CO2/EDGAR_2024_GHG_booklet_2024_fossilCO2only.xlsx", sheet_name="fossil_CO2_by_sector_country_su")

df_setor = edgar[['Sector']].dropna().drop_duplicates()
df_setor = df_setor.rename(columns={'Sector': 'Name'})

df_setor['Name'] = df_setor['Name'].str.slice(0, 100)

with engine.begin() as conn:
    df_setor.to_sql('Sector', conn, if_exists='append', index=False)

print("Tabela 'Setor' populada com sucesso! 🚀")
