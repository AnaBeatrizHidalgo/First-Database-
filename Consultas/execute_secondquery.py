import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

query = text('''
SELECT
    C."Name"                AS "Country",
    Y."year"                AS "Year",
    PS."Name"               AS "Energy Source",
    PS."Renewable",
    CPS."Power_Generation"  AS "Generation_TWh",
    CPS."CO2_Emission"      AS "Emissions_Mt",
    /*  g CO₂/kWh = (Mt × 10¹²) / (TWh × 10⁹) = (Mt / TWh) × 10³  */
    ROUND(
        (CPS."CO2_Emission" * 1000.0) / NULLIF(CPS."Power_Generation", 0),
        1
    )                       AS "gCO2_per_kWh"
FROM   "Country_Power Source" CPS
JOIN   "Country"      C  ON C."ID_Country"   = CPS."Country_ID_Country"
JOIN   "Year"         Y  ON Y."ID_year"      = CPS."Year_ID_year"
JOIN   "Power Source" PS ON PS."ID_Power"    = CPS."Power Source_ID_Power"
ORDER  BY Y."year" DESC, C."Name";

''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/secondQuery.csv")

    print(df.to_string(index=False))
