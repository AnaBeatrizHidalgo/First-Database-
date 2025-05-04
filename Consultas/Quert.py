import populate_scripts.ConectionBD as ConectionBD
from sqlalchemy import create_engime, Column, Integer, String, ForeignKey, text
from sqlalchemy.orm import relationship, sessionmaker, declarative_base

conn = ConectionBD.get_db_connection()
cursor = conn.cursor()

queryco2 = cursor.execute("SELECT " \
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
"ORDER BY E.CO2_Emission, [CO2 from Energy], [CO2 emission from others situations]")


queryInvestiment = cursor.execute("SELECT" \
"   C.Name AS Country," \
"   Y.year," \
"   I.Helth AS [Helth acess indicator]," \
"   G.Health_Expendicture AS [Investiment in helth]," \
"   E.CO2_Emission AS [CO2 emission]," \
"   I.Eletricty AS [Energy acess indicator]," \
"   G.Investiment_Energy AS [Investiment in energy]," \
"   SUM(CP.Power_Generation) AS [power generated]," \
"   P.GWH - P.PowerImport AS [Local energy use]," \
"   P.Renewable_Energy AS [Renewable Power used(%)]," \
"FROM Country_Year CY" \
"INNER JOIN Country C " \
"   ON CY.Country_ID_Country = C.ID_Country" \
"INNER JOIN Year Y" \
"   ON CY.Year_ID_Year = Y.ID_Year" \
"INNER JOIN GDP G" \
"   ON CY.GDP_ID_GDP = G.ID_GDP" \
"INNER JOIN IDH I" \
"   ON CY.IDH_ID_IDH = I.ID_IDH" \
"INNER JOIN [Environmental Indicator] E" \
"   ON CY.Enviromental_ID = E.ID_Environmental_Indicator" \
"INNER JOIN [Power Consumed] P" \
"   ON CY.ConsumePower_ID = P.ID_Consumed" \
"LEFT JOIN [Country_Power Source] AS CP" \
"   ON C.ID_Country = CP.Country_ID_Country" \
"LEFT JOIN [Power Soirce] PS" \
"   ON [CP.Power Source_ID_Power] = PS.ID_Power" \
"WHERE Y.year > 2020" \
"GROUP BY I.Helth, I.Eletricty, C.Name, Y.year, G.Health_Expendicture, E.CO2_Emission, G.Investiment_Energy, P.Renewable_Energy" \
"ORDER BY I.Helth, I.Eletricty")


"""Aqui falta organiza para conseguir fazer esses somatorios"""
queryEnergy = cursor.execute("SELECT" \
"   C.Name AS Country," \
"   Y.year," \
"   SUM(CP.Power_Generation) AS [renewble power generated]," \
"   SUM(CP.Power_Generation) AS [not renewble power generated]," \
"   P.GWH AS [Power Consumed]" \
"   P.PowerImport AS [Import Power use]," \
"   P.Renewable_Energy AS [Renewable Power used(%)]," \
"   I.Eletricty AS [Energy acess indicator]," \
"   G.Investiment_Energy AS [Investiment in energy]," \
"FROM Country_Year CY" \
"INNER JOIN Country C " \
"   ON CY.Country_ID_Country = C.ID_Country" \
"INNER JOIN Year Y" \
"   ON CY.Year_ID_Year = Y.ID_Year" \
"INNER JOIN GDP G" \
"   ON CY.GDP_ID_GDP = G.ID_GDP" \
"INNER JOIN IDH I" \
"   ON CY.IDH_ID_IDH = I.ID_IDH" \
"INNER JOIN [Environmental Indicator] E" \
"   ON CY.Enviromental_ID = E.ID_Environmental_Indicator" \
"INNER JOIN [Power Consumed] P" \
"   ON CY.ConsumePower_ID = P.ID_Consumed" \
"LEFT JOIN [Country_Power Source] AS CP" \
"   ON C.ID_Country = CP.Country_ID_Country" \
"LEFT JOIN [Power Soirce] PS" \
"   ON [CP.Power Source_ID_Power] = PS.ID_Power" \
"WHERE Y.year > 2020" \
"GROUP BY I.Eletricty, C.Name, Y.year, G.Investiment_Energy, P.Renewable_Energy" \
"ORDER BY G.Investiment_Energy, [renewble power generated]")
