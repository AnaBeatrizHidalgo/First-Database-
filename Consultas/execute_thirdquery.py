import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

query = text('''
SELECT  Y.year,
        S."Name"  AS Sector,
        SY."CO2_Emission"
FROM    "Sector_Year" SY
JOIN    "Year"        Y  ON SY."Year_ID_year" = Y."ID_year"
JOIN    "Sector"      S  ON SY."Sector_ID_Sector" = S."ID_Sector"
ORDER BY S."Name", Y.year;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/thirdQuery.csv")

    print(df.to_string(index=False))
