import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Analisar a ameissão de CO2 por pais e suas areas

query = text('''
SELECT C."Name"           AS "Country",
       PS."Year",
       E."CO2_Emision"    AS "Total Emission",
       SUM(PS."CO2_Emission")  AS "Emission Power",
       SUM(SC."CO2_Emission")  AS "Emission Sector"
FROM "Power Source_Country" PS
JOIN "Country" C ON PS."Country_ID_Country" = C."ID_Country"
JOIN "Environmental Indicator" E ON E."Country_ID" = C."ID_Country"  AND PS."Year" = E."Year"
JOIN "Sector_Country" SC ON SC."Country_ID_Country" = C."ID_Country"  AND PS."Year" = SC."Year"
GROUP BY "Country", "Total Emission", PS."Year"
ORDER BY PS."Year" DESC, "Total Emission" DESC, "Emission Power" DESC,"Emission Sector" DESC
LIMIT 100;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/thirdQuery.csv")

    print(df.to_string(index=False))
