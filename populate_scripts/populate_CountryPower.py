import psycopg2
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

# Configurações de conexão
DB_NAME = "ProjectBD"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost"
DB_PORT = "5432"

# 1. Conectar ao banco de dados
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# 2. Carregar o arquivo CSV
def load_csv_data(file_path):
    return pd.read_csv(file_path)

# 3. Obter mapeamentos de ID das tabelas de referência
def get_reference_mappings(conn):
    mappings = {}
    
    # Mapeamento de países
    with conn.cursor() as cur:
        cur.execute("""SELECT "ID_Country", "Name" FROM "Country" """)
        mappings['countries'] = {row[1]: row[0] for row in cur.fetchall()}
    
    # Mapeamento de anos
    with conn.cursor() as cur:
        cur.execute("""SELECT "ID_year", "year" FROM "Year" """)
        mappings['years'] = {row[1]: row[0] for row in cur.fetchall()}
    
    # Mapeamento de fontes de energia
    with conn.cursor() as cur:
        cur.execute("""SELECT "ID_Power", "Name" FROM "Power Source" """)
        mappings['power_sources'] = {row[1]: row[0] for row in cur.fetchall()}
    
    return mappings

# 4. Processar os dados do CSV e preparar para inserção
def process_data(df, mappings):
    # Verificar e tratar valores ausentes
    df.fillna(0, inplace=True)
    
    # Adicionar colunas de ID
    df["Country_ID_Country"] = df['Country'].map(mappings['countries'])
    df["Year_ID_year"] = df['Year'].map(mappings['years'])
    df["Power Source_ID_Power"] = df['Power Source'].map(mappings['power_sources'])
    
    # Verificar mapeamentos ausentes
    missing_countries = df[df["Country_ID_Country"].isna()]["Country"].unique()
    missing_years = df[df["Year_ID_year"].isna()]["Year"].unique()
    missing_sources = df[df["Power Source_ID_Power"].isna()]["Power Source"].unique()
    
    if missing_countries.any():
        print(f"Aviso: Países não encontrados no banco: {missing_countries}")
    if missing_years.any():
        print(f"Aviso: Anos não encontrados no banco: {missing_years}")
    if missing_sources.any():
        print(f"Aviso: Fontes de energia não encontradas no banco: {missing_sources}")
    
    # Filtrar apenas linhas com todos os mapeamentos válidos
    df = df.dropna(subset=["Country_ID_Country", "Year_ID_year", "Power Source_ID_Power"])
    
    # Converter tipos
    df["Country_ID_Country"] = df["Country_ID_Country"].astype(int)
    df["Year_ID_year"] = df["Year_ID_year"].astype(int)
    df["Power Source_ID_Power"] = df["Power Source_ID_Power"].astype(int)
    
    # Selecionar colunas para inserção
    result_df = df[[
        "Country_ID_Country", 
        "Year_ID_year", 
        "Power Source_ID_Power", 
        "Power_Generation", 
        "CO2_Emission"
    ]]
    
    return result_df

# 5. Inserir dados no banco usando COPY
def insert_data_with_copy(conn, df):
    # Criar um buffer de string para o COPY
    buffer = StringIO()
    
    # Escrever os dados no buffer no formato CSV
    df.to_csv(buffer, sep='\t', header=False, index=False)
    buffer.seek(0)
    
    with conn.cursor() as cur:
        # Criar tabela temporária para carregamento
        cur.execute("""
            CREATE TEMP TABLE temp_country_year (
                "Country_ID_Country" INT,
                "Year_ID_year" INT,
                "Power Source_ID_Power" INT,
                "Power_Generation" FLOAT,
                "CO2_Emission" FLOAT
            )
        """)
        
        # Carregar dados na tabela temporária
        cur.copy_expert("""
            COPY temp_country_year FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER '\t',
                NULL ''
            )
        """, buffer)
        
        # Inserir na tabela final, evitando duplicatas
        cur.execute("""
            INSERT INTO "Country_Power Source" (
                "Country_ID_Country", 
                "Year_ID_year", 
                "Power Source_ID_Power", 
                "Power_Generation", 
                "CO2_Emission"
            )
            SELECT 
                "Country_ID_Country",
                "Year_ID_year",
                "Power Source_ID_Power",
                "Power_Generation",
                "CO2_Emission"
            FROM temp_country_year
            ON CONFLICT DO NOTHING
        """)
        
        # Limpar tabela temporária
        cur.execute("DROP TABLE temp_country_year")
        
        conn.commit()

# Função principal
def main(csv_file_path):
    conn = None
    try:
        # Estabelecer conexão
        conn = get_db_connection()
        
        # Carregar dados do CSV
        df = load_csv_data(csv_file_path)
        
        # Obter mapeamentos de ID
        mappings = get_reference_mappings(conn)
        
        # Processar dados
        processed_df = process_data(df, mappings)
        
        # Inserir dados no banco
        insert_data_with_copy(conn, processed_df)
        
        print("Dados importados com sucesso!")
        
    except Exception as e:
        print(f"Erro durante a importação: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# Executar
if __name__ == "__main__":
    csv_file_path = "./Datasets/EnergyData.csv"
    main(csv_file_path)