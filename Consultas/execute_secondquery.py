import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

query = text('''
SELECT  C."Name" AS Country,
        Y.year,
        PS."Name" AS Energy,
        PS."Renewable",
        CPS."Power_Generation",
        CPS."CO2_Emission" 
FROM    "Country_Power Source" CPS
JOIN    "Country"      C  ON CPS."Country_ID_Country" = C."ID_Country"
JOIN    "Year"         Y  ON Y.year                   = CPS."Year_ID_year"
JOIN    "Power Source" PS ON CPS."Power Source_ID_Power" = PS."ID_Power"
ORDER BY Y.year DESC, C."Name";
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/secondQuery.csv")

    print(df.to_string(index=False))
