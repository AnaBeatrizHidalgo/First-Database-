import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Analisar a ameissão de CO2 por pais

query = text('''
SELECT C."Nome" AS Country,
       PS."Year", 
       P."Name"           AS Power Generations,
       PS."CO2_Emissions" AS Emission Power,
       S."Name"           AS Sector,
       SC."CO2_Emission"  AS Emission Sector,
       E."CO2_Emision"    AS Total Emission
FROM "Country_Power Source" PS
JOIN "Country" C ON PS."Country_ID" = C."ID_Country"
JOIN "Power Source" P ON P."ID_Power" = PS."Power Source_ID_Power"
JOIN "Environmental Indicator" E ON E."Country_ID" = C."ID_Country"  AND PS."Year" = E."Year"
JOIN "Sector_Country" SC ON SC."Country_ID" = C."ID_Country"  AND PS."Year" = SC."Year"
Join "Sector" S ON S."ID_Sector" = SC."Sector_ID"
ORDER BY Emission Power DESC, Emission Sector DESC, total_emissio DESC
LIMIT 100;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/thirdQuery.csv")

    print(df.to_string(index=False))
