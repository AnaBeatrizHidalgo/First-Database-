import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

file_path = '../Datasets/Dados CO2/EDGAR_2024_GHG_booklet_2024_fossilCO2only.xlsx'

sheet_name = 'fossil_CO2_by_sector_country_su'
df = pd.read_excel(file_path, sheet_name=sheet_name)

df.columns = df.columns.map(lambda x: str(x).strip())
df['EDGAR Country Code'] = df['EDGAR Country Code'].astype(str).str.strip().str.upper()

df_global = df[df['EDGAR Country Code'] == 'GLOBAL TOTAL']

print(f"Linhas encontradas como 'GLOBAL TOTAL': {len(df_global)}")
if df_global.empty:
    raise ValueError("Nenhuma linha com 'GLOBAL TOTAL' encontrada. Verifique o dataset!")

years = [str(year) for year in range(2000, 2024)]
available_years = [year for year in years if year in df_global.columns]

df_global = df_global[['Sector'] + available_years]

sector_id_map = {
    'Agriculture': 1,
    'Buildings': 2,
    'Fuel Exploitation': 3,
    'Industrial Combustion': 4,
    'Power Industry': 5,
    'Processes': 6,
    'Transport': 7,
    'Waste': 8
}

year_id_map = {year: idx + 1 for idx, year in enumerate(range(2000, 2024))}

records = []
for _, row in df_global.iterrows():
    sector = row['Sector']
    setor_id = sector_id_map.get(sector)
    if setor_id is None:
        continue
    for year in available_years:
        emission = row[year]
        if pd.notnull(emission):
            records.append({
                'Sector_ID_Sector': setor_id,
                'Year_ID_year': year_id_map[int(year)],
                'CO2_Emission': int(round(emission))
            })

df_final = pd.DataFrame(records)
print(df_final)

print(f"Total de registros para inserir: {len(df_final)}")
print(df_final.head())

if not df_final.empty:
    with engine.begin() as conn:
        df_final.to_sql('Sector_Year', conn, if_exists='append', index=False)
    print("Tabela 'Sector_Year' populada com sucesso! 🚀")
else:
    print("Nenhum dado foi inserido porque o DataFrame final estava vazio.")
