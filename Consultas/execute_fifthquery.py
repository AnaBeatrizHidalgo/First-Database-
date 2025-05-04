import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

query = text('''
SELECT  C."Name"            AS country,
        Y.year,
        I."Electricity"     AS electricity_index,
        G."Investment_Energy",
       SUM(CP."Power_Generation") AS total_power_generation,
        P."Renewable_Energy" AS renewable_share_pct,
        GREATEST(P."PowerImport", 0)      AS import_gwh,
        GREATEST(-P."PowerImport", 0)     AS export_gwh,
        P."PowerImport"                   AS net_import_gwh
FROM    "Country_Year"  CY
JOIN    "Country"       C  ON C."ID_Country" = CY."Country_ID_Country"
JOIN    "Year"          Y  ON Y."ID_year"    = CY."Year_ID_year"
LEFT JOIN "IDH"         I  ON I."ID_IDH"     = CY."IDH_ID"
LEFT JOIN "GDP"         G  ON G."ID_GDP"     = CY."GDP_ID"
LEFT JOIN "Power Consumed" P ON P."ID_Consumed" = CY."ConsumePower_ID"
LEFT JOIN "Country_Power Source" CP ON  CP."Country_ID_Country" = CY."Country_ID_Country"
                                        AND CP."Year_ID_year" = CY."Year_ID_year"
WHERE   I."Electricity" IS NOT NULL AND G."Investment_Energy" IS NOT NULL
GROUP BY C."Name", Y.year, I."Electricity", G."Investment_Energy", P."Renewable_Energy", P."PowerImport"
ORDER BY electricity_index DESC, G."Investment_Energy" DESC;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/fifthQuery.csv")

    print(df.to_string(index=False))