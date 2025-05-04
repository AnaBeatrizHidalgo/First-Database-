import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

query = text('''
WITH country_stats AS (
    SELECT  C."Name"                                AS label,
            Y.year,
            'country'                               AS level,
            EI."CO2_Emision"         AS co2_mt,
            G."GDP"                  AS gdp_musd,
            ROUND((EI."CO2_Emision" * 1000)
                  / NULLIF(G."GDP",0), 4)           AS tco2_per_musd
    FROM    "Country_Year"           CY
    JOIN    "Country"                C  ON C."ID_Country"   = CY."Country_ID_Country"
    JOIN    "Year"                   Y  ON Y."ID_year"      = CY."Year_ID_year"
    LEFT JOIN "Environmental Indicator" EI ON EI."ID_Environmental" = CY."Environmental_ID"
    LEFT JOIN "GDP"                   G  ON G."ID_GDP"      = CY."GDP_ID"
    WHERE   EI."CO2_Emision" IS NOT NULL
      AND   G."GDP"          IS NOT NULL
),

world_gdp AS (
    SELECT  Y.year,
            SUM(G."GDP")            AS gdp_musd
    FROM    "Country_Year" CY
    JOIN    "Year"         Y ON Y."ID_year" = CY."Year_ID_year"
    JOIN    "GDP"          G ON G."ID_GDP"  = CY."GDP_ID"
    WHERE   G."GDP" IS NOT NULL
    GROUP BY Y.year
),

sector_stats AS (
    SELECT  S."Name"                               AS label,
            Y.year,
            'sector'                               AS level,
            SY."CO2_Emission"        AS co2_mt,
            wg.gdp_musd,
            ROUND((SY."CO2_Emission" * 1000)
                  / NULLIF(wg.gdp_musd,0), 4)      AS tco2_per_musd
    FROM    "Sector_Year" SY
    JOIN    "Sector"       S  ON S."ID_Sector"      = SY."Sector_ID_Sector"
    JOIN    "Year"         Y  ON Y."ID_year"        = SY."Year_ID_year"
    JOIN    world_gdp      wg ON wg.year            = Y.year
    WHERE   SY."CO2_Emission" IS NOT NULL
)

SELECT *
FROM   country_stats

UNION ALL

SELECT *
FROM   sector_stats

ORDER BY level, tco2_per_musd DESC, year;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

df.to_csv("./Consultas/fourthQuery.csv")
