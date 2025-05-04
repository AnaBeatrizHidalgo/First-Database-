import ConectionBD
from sqlalchemy import create_engine, text

conn = ConectionBD.get_db_connection()

cursor = conn.cursor()

sql1 = """
COPY "Sector"("ID_Sector", "Name", "Descrition")
FROM STDIN WITH(
    FORMAT CSV,
    HEADER,
    DELIMITER ','
    )
"""

sql2 = """
COPY "Region"("ID_Region", "Name")
FROM STDIN WITH(
    FORMAT CSV,
    HEADER,
    DELIMITER ','
    )
"""

sql3 = """
COPY "Year"("ID_year", "year")
FROM STDIN WITH(
    FORMAT CSV,
    HEADER,
    DELIMITER ','
    )
"""

sql4 = """
COPY "Power Source"("ID_Power", "Name", "Renewable")
FROM STDIN WITH(
    FORMAT CSV,
    HEADER,
    DELIMITER ','
    )
"""


with open('./Datasets/Sector.csv', 'r', encoding= 'utf-8') as s:
    cursor.copy_expert(sql1,s)

with open('./Datasets/Region.csv', 'r', encoding= 'utf-8') as c:
    cursor.copy_expert(sql2,c)

with open('./Datasets/Year.csv', 'r', encoding= 'utf-8') as y:
    cursor.copy_expert(sql3,y)

with open('./Datasets/Power.csv', 'r', encoding= 'utf-8') as p:
    cursor.copy_expert(sql4,p)

conn.commit()
conn.close()
