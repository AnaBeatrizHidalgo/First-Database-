import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

# Query para analisar quanto a produção de energia reflete na sua utilização

query = text('''
SELECT  C."Name" AS country,
        CP."Year",
        I."Investiment_Energy",
        SUM(CASE WHEN PS."Renewable" = TRUE THEN CP."Power_Generation" ELSE 0 END) AS renewable_power,
    	SUM(CASE WHEN PS."Renewable" = FALSE THEN CP."Power_Generation" ELSE 0 END) AS non_renewable_power,
        P."GWH"                AS power_consumed,
		P."Renewable_Energy"   AS renewable_share_pct,
		P."PowerImport"        AS power_import_consumed,
        D."Eletricity" 		   AS access_to_energy
FROM    "Country"  C
LEFT JOIN "Development"         D  ON D."Country_ID" = C."ID_Country" AND D."Year" = CP."Year"
LEFT JOIN "Investiment"         I  ON I."ID_GDP" = C."ID_Country" AND I."Year" = CP."Year"
LEFT JOIN "Power Consumed" P ON P."Country_ID" = C."ID_Country" AND P."Year" = CP."Year"
LEFT JOIN "Country_Power Source" CP ON  CP."Country_ID_Country" = C."ID_Country"
LEFT JOIN "Power Source" PS ON CP."Power Source_ID_Power" = PS."ID_Power"
WHERE CP."Year" > 2010 AND D."Eletricity" IS NOT NULL AND I."Investiment_Energy" IS NOT NULL
GROUP BY C."Name", Y.year, D."Eletricity", G."Investment_Energy", P."Renewable_Energy", P."PowerImport", P."GWH"
ORDER BY I."Investiment_Energy" DESC, renewable_power DESC, D."Eletricity" DESC;
''')

with engine.begin() as conn:
    df = pd.read_sql(query, conn)
    conn.close()

    df.to_csv("./Consultas/sixthQuery.csv")

    print(df.to_string(index=False))
