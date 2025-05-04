import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

query = text('''
SELECT  C."Name"           AS country,
        Y.year,
        G."GDP",
        I."IDH",
        E."CO2_Emision",             
        P."GWH",
        CY."Population"
FROM    "Country_Year"          CY
JOIN    "Country"               C  ON CY."Country_ID_Country" = C."ID_Country"
JOIN    "Year"                  Y  ON CY."Year_ID_year"       = Y."ID_year"
JOIN    "GDP"                   G  ON CY."GDP_ID"             = G."ID_GDP"
JOIN    "IDH"                   I  ON CY."IDH_ID"             = I."ID_IDH"
JOIN    "Environmental Indicator" E ON CY."Environmental_ID"   = E."ID_Environmental"
JOIN    "Power Consumed"        P  ON CY."ConsumePower_ID"    = P."ID_Consumed"
ORDER BY C."Name", Y.year, CY."Population";
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/thirdQuery.csv")

    print(df.to_string(index=False))
