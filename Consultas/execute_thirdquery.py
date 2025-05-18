import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

query = text('''
WITH country_totals AS (
    SELECT
        scy."Country_ID_Country",
        SUM(scy."CO2_Emission")          AS total_emission_2023
    FROM public."Sector_Country_Year" scy
    JOIN public."Year"   y ON y."ID_year" = scy."Year_ID_year"
    WHERE y."year" = 2023
    GROUP BY scy."Country_ID_Country"
    ORDER BY total_emission_2023 DESC
),
top_sectors AS (
    SELECT DISTINCT ON (scy."Country_ID_Country")
        scy."Country_ID_Country",
        scy."Sector_ID_Sector",
        scy."CO2_Emission"               AS sector_emission_2023
    FROM public."Sector_Country_Year" scy
    JOIN public."Year" y ON y."ID_year" = scy."Year_ID_year"
    WHERE y."year" = 2023
      AND scy."Country_ID_Country" IN (SELECT "Country_ID_Country" FROM country_totals)
    ORDER BY scy."Country_ID_Country", scy."CO2_Emission" DESC   -- pega o maior setor de cada país
)
SELECT
    c."Name"                    AS "Country",
    ct.total_emission_2023      AS "Total CO₂ 2023 (Mt)",
    s."Name"                    AS "Top Sector 2023",
    ts.sector_emission_2023     AS "Sector CO₂ 2023 (Mt)",
    ROUND(100.0 * ts.sector_emission_2023 / ct.total_emission_2023, 1) || '%' AS "Share"
FROM country_totals  ct
JOIN top_sectors     ts ON ts."Country_ID_Country" = ct."Country_ID_Country"
JOIN public."Country" c ON c."ID_Country"          = ct."Country_ID_Country"
JOIN public."Sector"  s ON s."ID_Sector"           = ts."Sector_ID_Sector"
ORDER BY ct.total_emission_2023 DESC;

''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/thirdQuery.csv")

    print(df.to_string(index=False))
