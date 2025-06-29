import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Fazer uma analise global da emissão de CO2, do desenvolvimento e do investimento, procurando indicar qual o maior causador de cada tipo de emissao

query = text('''
WITH CO2_sector AS (
    SELECT  S."Name"                 AS sector,
            CS."Year",
            SUM(CS."CO2_Emission")    AS co2_sector
    FROM    "Sector_Country" CS
    JOIN    "Sector"         S  ON S."ID_Sector" = CS."Sector_ID_Sector"  
    GROUP BY sector, CS."Year"
),

CO2_energy AS (
    SELECT PS."Name" AS energy_name,
           PS."Renewable",
           PSC."Year",
           SUM(PSC."CO2_Emission") AS co2_energy
    FROM "Power Source_Country" PSC
    JOIN "Power Source" PS ON PS."ID_Power" = PSC."Power Source_ID_Power"
    GROUP BY PS."Name", PS."Renewable", PSC."Year"
),

CO2_country AS (
    SELECT  C."Name"            AS country,
            EI."Year",
            EI."CO2_Emision"   AS co2_country
    FROM    "Country" C
    JOIN    "Environmental Indicator" EI  ON C."ID_Country" = EI."Country_ID"
),

IDH_total AS (
    SELECT SUM(D."IDH") AS total_idh,
           D."Ano"      AS "Year"
    FROM "Development" D
    GROUP BY D."Ano"
),

GDP_total AS (
    SELECT SUM(I."GDP") AS total_gdp,
           I."Year"
    FROM "Investment" I
    GROUP BY I."Year"
),
             

year AS (
    SELECT DISTINCT "Year" FROM "Sector_Country"
    UNION
    SELECT DISTINCT "Year" FROM "Power Source_Country"
    UNION
    SELECT DISTINCT "Ano" AS "Year" FROM "Development"
    UNION
    SELECT DISTINCT "Year" FROM "Investment"
),

resultados AS (
    SELECT 
        y."Year" AS "Year",
        (SELECT SUM(co2_sector) FROM co2_sector AS cs WHERE cs."Year" = y."Year") AS co2_total_sector,
        (SELECT sector FROM co2_sector  AS cs WHERE cs."Year" = y."Year" ORDER BY co2_sector DESC LIMIT 1) AS sector_maior_emissao,
        (SELECT SUM(co2_energy) FROM co2_energy AS ce WHERE ce."Year" = y."Year") AS co2_total_energia,
        (SELECT energy_name FROM co2_energy AS ce WHERE ce."Year" = y."Year" ORDER BY co2_energy DESC LIMIT 1) AS energia_maior_emissao,
        (SELECT SUM(co2_country) FROM co2_country  AS cc WHERE cc."Year" = y."Year") AS co2_total_country,
        (SELECT country FROM co2_country  AS cc WHERE cc."Year" = y."Year" ORDER BY co2_country DESC LIMIT 1) AS country_maior_emissao,
        (SELECT total_idh FROM IDH_total AS i WHERE i."Year" = y."Year") AS total_IDH,
        (SELECT total_gdp FROM GDP_total AS g  WHERE g."Year" = y."Year") AS total_GDP
    FROM year AS y
)

SELECT *
FROM resultados
WHERE co2_total_sector IS NOT NULL 
  AND co2_total_energia IS NOT NULL 
  AND co2_total_country IS NOT NULL 
  AND total_IDH IS NOT NULL 
  AND total_GDP IS NOT NULL
ORDER BY "Year" DESC, co2_total_country DESC, co2_total_energia DESC, co2_total_sector DESC
LIMIT 30;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/fourthQuery.csv")

    print(df.to_string(index=False))
