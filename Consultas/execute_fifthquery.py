import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Saber de quanto que o pais investe reflete no seu desenvolvimento

query = text('''
SELECT  C."Name"            AS "country",
        I."Year",
        D."IDH",
        I."GDP",
        D."Electricity"     AS "electricity index",
        I."Investment_Energy" AS  "eletricity investiment",
        D."Health"          AS "health index",
        I."Health_Expenditure" AS "helth investiment",
        SUM(CP."Power_Generation") AS "total_power_generation",
        P."Renewable_Energy" AS "renewable share pct",
        P."PowerImport"      AS "import_gwh",
        E."CO2_Emision"      AS "total_emission",
        SUM(SC."CO2_Emission") AS "sector emission", 
        SUM(CP."CO2_Emission") AS "emission energy"
FROM    "Country"  C
JOIN "Investment"     I  ON I."Country_ID" =  C."ID_Country"
JOIN "Development"     D  ON D."Country_ID" =  C."ID_Country" AND I."Year" = D."Ano"
JOIN "Environmental Indicator" E ON E."Country_ID" = C."ID_Country"  AND I."Year" = E."Year"
JOIN "Power Consumed" P ON P."Country_ID" = C."ID_Country"  AND I."Year" = P."Year"
JOIN "Power Source_Country" CP ON  CP."Country_ID_Country" = C."ID_Country"  AND I."Year" = CP."Year"
JOIN "Sector_Country" SC ON SC."Country_ID_Country" = C."ID_Country" AND I."Year" = SC."Year"
WHERE D."IDH" IS NOT NULL AND I."GDP" IS NOT NULL AND D."Electricity" IS NOT NULL AND D."Health" IS NOT NULL
GROUP BY C."Name", I."Year", D."IDH", D."Health", D."Electricity",
         I."GDP", I."Health_Expenditure", I."Investment_Energy", 
         P."Renewable_Energy", P."PowerImport", E."CO2_Emision"
ORDER BY D."IDH" DESC, I."GDP" DESC, total_emission DESC, I."Year" DESC
LIMIT 50;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/fifthQuery.csv")

    print(df.to_string(index=False))