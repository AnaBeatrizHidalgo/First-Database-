"""Consultas iniciais para analisar de maneira geral as informações do nosso banco"""
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
"   CY.Population" \
"   G.GDP," \
"   I.IDH," \
"   E.CO2_Emission," \
"FROM Country_Year CY" \
"INNER JOIN Country C " \
"   ON CY.Country_ID_Country = C.ID_Country" \
"INNER JOIN Year Y " \
"   ON CY.Year_ID_Year = Y.ID_Year" \
"INNER JOIN GDP G" \
"   ON CY.GDP_ID_GDP = G.ID_GDP" \
"INNER JOIN IDH I" \
"   ON CY.IDH_ID_IDH = I.ID_IDH" \
"INNER JOIN [Environmental Indicator] E" \
"   ON CY.Enviromental_ID = E.ID_Environmental_Indicator" \
"ORDER BY C.Name, Y.year AND CY.Pouplation")

secondQuery = cursor.execute("SELECT " \
    "C.Name AS Country,"\
    "Y.year," \
    "S.Name AS Sector" \
    "SY.CO2_Emission" \
"From Sector_Year SY" \
"INNER JOIN Country C" \
"       ON SY.Country_ID_Country = C.ID_Country" \
"INNER JOIN Year Y" \
"       ON SY.Year_ID_Year = Y.ID_Year" \
"INNER JOIN Sector S" \
"       ON SY.Sector_ID_Sector = S.ID_Sector" \
"ORDER BY C.Name, Y.year, S.Name")

thirdQuery = cursor.execute("SELECT" \
"   C.Name AS Country" \
"   Y.year" \
"   P.Name AS Energy" \
"   CP.Power_Generati" \
"   CP.CO2_Emission" \
"FROM [Country_Power Source] CP" \
"INNER JOIN Country C" \
"       ON CP.Country_ID_Country = C.ID_Country" \
"INNER JOIN Year Y" \
"       ON CP.Year_ID_year = Y.ID_year" \
"INNER JOIN [Power Source] P" \
"       ON [CP.Power Source_ID_Power] = [P.ID_Power]" \
"ORDER BY C.Name, Y.year, P.Name")
