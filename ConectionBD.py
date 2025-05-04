import psycopg2

# Conexão ao banco de dados
def get_db_connection():
    return psycopg2.connect(
        dbname="DB_NAME",
        user="DB_USER",
        password="DB_PASSWORD",
        host="DB_HOST",
        port="5432"
    )

