import psycopg2

# Conexão ao banco de dados
def get_db_connection():
    return psycopg2.connect(
        dbname="Projeto1",
        user="postgres",
        password="123",
        host="localhost",
        port="5432"
    )

