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
        cur.execute("""SELECT "ID_Region", "Name" FROM "Region" """)
        mappings['regions'] = {row[1]: row[0] for row in cur.fetchall()}
    
    return mappings

# 4. Processar os dados do CSV e preparar para inserção
def process_data(df, mappings):
    # Verificar e tratar valores ausentes
    df.fillna(0, inplace=True)
    
    # Adicionar colunas de ID
    df['Region_ID_Region'] = df["Region"].map(mappings['regions'])
    
    # Verificar mapeamentos ausentes
    missing_regions = df[df["Region_ID_Region"].isna()]["Region"].unique()
    
    if missing_regions.any():
        print(f"Aviso: Regiões não encontrados no banco: {missing_regions}")
    
    # Filtrar apenas linhas com todos os mapeamentos válidos
    df = df.dropna(subset=["Region_ID_Region"])
    
    # Converter tipos
    df["Region_ID_Region"] = df["Region_ID_Region"].astype(int)
    
    # Selecionar colunas para inserção
    result_df = df[[
        'ID_Country', 
        'Name',
        'Region_ID_Region'
    ]]
    
    return result_df

# 5. Inserir dados no banco usando COPY
def insert_data_with_copy(conn, df):
    buffer = StringIO()
    df.to_csv(buffer, sep='\t', header=False, index=False)
    buffer.seek(0)
    
    with conn.cursor() as cur:
        # Verifique se os nomes das colunas correspondem exatamente
        cur.execute("""
            CREATE TEMP TABLE temp_country (
                "ID_Country" INT,
                "Name" VARCHAR,
                "Region_ID_Region" INT
            )
        """)
        
        cur.copy_expert("COPY temp_country FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')", buffer)
        
        cur.execute("""
            INSERT INTO "Country" (
                "ID_Country",
                "Name",
                "Region_ID_Region"
            )
            SELECT 
                "ID_Country",
                "Name",
                "Region_ID_Region"
            FROM temp_country
            ON CONFLICT DO NOTHING
        """)
        
        cur.execute("DROP TABLE temp_country")
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
    csv_file_path = "./Datasets/Country.csv"
    main(csv_file_path)