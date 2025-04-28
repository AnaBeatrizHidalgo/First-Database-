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

# Limpar colunas e valores
df.columns = df.columns.map(lambda x: str(x).strip())
df['EDGAR Country Code'] = df['EDGAR Country Code'].astype(str).str.strip().str.upper()

# Filtrar apenas GLOBAL TOTAL
df_global = df[df['EDGAR Country Code'] == 'GLOBAL TOTAL']

# Debug: verificação
print(f"Linhas encontradas como 'GLOBAL TOTAL': {len(df_global)}")
if df_global.empty:
    raise ValueError("Nenhuma linha com 'GLOBAL TOTAL' encontrada. Verifique o dataset!")

# Anos disponíveis: de 2000 até 2023
years = [str(year) for year in range(2000, 2024)]
available_years = [year for year in years if year in df_global.columns]

# Selecionar colunas
df_global = df_global[['Sector'] + available_years]

# Mapear setores para IDs
sector_id_map = {
    'Agriculture': 2,
    'Buildings': 3,
    'Fuel Exploitation': 4,
    'Industrial Combustion': 5,
    'Power Industry': 6,
    'Processes': 7,
    'Transport': 8,
    'Waste': 9
}

# Mapear anos para IDs
year_id_map = {year: idx + 1 for idx, year in enumerate(range(2000, 2024))}

# Montar registros para inserção
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
                'Setor_ID_Server': setor_id,
                'Year_ID_year': year_id_map[int(year)],
                'CO2_Emission': int(round(emission))
            })

# Criar DataFrame final
df_final = pd.DataFrame(records)

# Debug final
print(f"Total de registros para inserir: {len(df_final)}")
print(df_final.head())

# Inserir no banco
if not df_final.empty:
    with engine.begin() as conn:
        df_final.to_sql('Setor_Year', conn, if_exists='append', index=False)
    print("Tabela 'Setor_Year' populada com sucesso! 🚀")
else:
    print("Nenhum dado foi inserido porque o DataFrame final estava vazio.")
