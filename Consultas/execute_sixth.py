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
        SUM(CASE WHEN PS."Renewable" = TRUE THEN CP."Power_Generation" ELSE 0 END) AS renewable_poweR,
    	SUM(CASE WHEN PS."Renewable" = FALSE THEN CP."Power_Generation" ELSE 0 END) AS non_renewable_poweR,
		p."Renewable_Energy"  AS renewable_share_pct,
		P."PowerImport",
		I."Health" 			AS health_index,
		G."Health_Expenditure" ,
		I."Sanitation",
		E."CO2_Emision"
FROM    "Country_Year"  CY
JOIN    "Country"       C  ON C."ID_Country" = CY."Country_ID_Country"
JOIN    "Year"          Y  ON Y."ID_year"    = CY."Year_ID_year"
LEFT JOIN "IDH"         I  ON I."ID_IDH"     = CY."IDH_ID"
LEFT JOIN "GDP"         G  ON G."ID_GDP"     = CY."GDP_ID"
LEFT JOIN "Environmental Indicator" E ON E."ID_Environmental" = CY."Environmental_ID"
LEFT JOIN "Power Consumed" P ON P."ID_Consumed" = CY."ConsumePower_ID"
LEFT JOIN "Country_Power Source" CP ON  CP."Country_ID_Country" = CY."Country_ID_Country"
                                        AND CP."Year_ID_year" = CY."Year_ID_year"
LEFT JOIN "Power Source" PS ON CP."Power Source_ID_Power" = PS."ID_Power"
WHERE   I."Electricity" IS NOT NULL AND G."Investment_Energy" IS NOT NULL AND Y.year > 2010
GROUP BY C."Name", Y.year, I."Electricity", G."Investment_Energy", P."Renewable_Energy", P."PowerImport", I."Health", G."Health_Expenditure", I."Sanitation", E."CO2_Emision"
ORDER BY E."CO2_Emision" DESC, I."Health" DESC;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/sixthQuery.csv")

    print(df.to_string(index=False))
