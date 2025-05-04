import ConectionBD
from sqlalchemy import create_engine, text

conn = ConectionBD.get_db_connection()

cursor = conn.cursor()

sql4 = """
COPY "Power Source"("ID_Power", "Name", "Renewable")
FROM STDIN WITH(
    FORMAT CSV,
    HEADER,
    DELIMITER ','
    )
"""

with open('../Datasets/Power.csv', 'r', encoding= 'utf-8') as p:
    cursor.copy_expert(sql4,p)

conn.commit()
conn.close()
