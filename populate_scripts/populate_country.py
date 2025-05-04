import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

countries = pd.read_csv("../Datasets/WDICountry.csv")

df_country = countries[['Table Name']].rename(columns={
    'Table Name': 'Name',
})

df_country['Name'] = df_country['Name'].str.slice(0, 100)

with engine.begin() as conn:
    df_country.to_sql('Country', conn, if_exists='append', index=False)

print("Tabela 'Country' populada com sucesso! 🚀")
