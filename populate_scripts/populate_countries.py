import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Conectar no banco
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Caminho para o arquivo
file_path = './EDGAR_2024_GHG_booklet_2024_fossilCO2only.xlsx'

# Carregar a planilha
sheet_name = 'fossil_CO2_by_sector_country_su'
df = pd.read_excel(file_path, sheet_name=sheet_name)

# Filtrar apenas "GLOBAL TOTAL"
global_total_df = df[df['EDGAR Country Code'] == 'GLOBAL TOTAL']

# Anos de interesse
years = [str(year) for year in range(2000, 2026)]
available_years = [year for year in years if year in global_total_df.columns]

# Selecionar colunas relevantes
relevant_columns = ['Sector'] + available_years
global_total_df = global_total_df[relevant_columns]

# Mapeamento dos setores para IDs
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

# Mapeamento dos anos para IDs (2000 -> 1, ..., 2025 -> 26)
year_id_map = {year: idx + 1 for idx, year in enumerate(range(2000, 2026))}

# Construir o DataFrame final para inserir
records = []
for _, row in global_total_df.iterrows():
    sector = row['Sector']
    setor_id = sector_id_map.get(sector)
    if setor_id is None:
        continue
    for year in available_years:
        emission = row[year]
        if pd.notnull(emission):
            records.append({
                'Setor_ID_Server': setor_id,
                'Year_ID_year': year_id_map[int(year)],
                'CO2_Emission': int(round(emission))
            })

df_final = pd.DataFrame(records)

# Inserir no banco
with engine.begin() as conn:
    df_final.to_sql('Setor_Year', conn, if_exists='append', index=False)

print("Tabela 'Setor_Year' populada com sucesso! 🚀")
