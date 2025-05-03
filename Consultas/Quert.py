"""
Fazer uma analise em cima das emisões de CO2
Somar emissão do setor e da energia gerada, para comparar com o total e com o de mudança de terra
"""

import psycopg2
from sqlalchemy import create_engime, Column, Integer, String, ForeignKey, text
from sqlalchemy.orm import relationship, sessionmaker, declarative_base

conn = psycopg2.connect(
    dbname= "MC536",
    user= "postgres",
    password= "123",
    host= "localhost",
    port= "5432"
)

cursor = conn.cursor()

firstQuery = cursor.execute("SELECT " \
"   C.Name AS Country," \
"   Y.year," \
"   SUM(SY.CO2_Emission) AS [CO2 from Setor]," \
"   SUM(CP.CO2_Emission) AS [CO2 from Energy]," \
"   E.ELUC AS [CO2 emission from land-use change]," \
"   E.CO2_Emission AS [Total CO2 Emission]," \
"   E.CO2_Emission - SUM(SY.CO2_Emission) - SUM(CP.CO2_Emission) AS [CO2 emission from others situations]" \
"FROM Sector_Year SY" \
"INNER JOIN Country C " \
"   ON SY.Country_ID_Country = C.ID_Country" \
"INNER JOIN Year Y" \
"   ON SY.Year_ID_Year = Y.ID_Year" \
"LEFT JOIN Country_Power_Source CP" \
"   ON SY.Country_ID_Country = CP.Country_ID_Country AND SY.Year_ID_Year = CP.Year_ID_Year" \
"LEFT JOIN Country_Year CY" \
"   ON SY.Country_ID_Country = CY.Country_ID_Country AND SY.Year_ID_Year = CY.Year_ID_Year" \
"LEFT JOIN [Environmental Indicator] E" \
"   ON CY.Enviromental_ID = E.ID_Environmental" \
"GROUP BY C.Name, Y.year, E.CO2_Emission, E.ELUC" \
"ORDER BY C.Name,  Y.year, E.CO2_Emission")

