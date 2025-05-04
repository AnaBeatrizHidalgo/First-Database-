import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_ENV = Path("../Datasets/CombinandoEnviromental.csv")  
YEAR_MIN, YEAR_MAX = 2000, 2025                  

def clean_country(val) -> str:
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()

def parse_number(val):
    if pd.isna(val):
        return None
    m = re.search(r'[-+]?\d+(?:[.,]\d+)?', str(val).replace(',', '.'))
    return float(m.group()) if m else None

def py_val(v):
    if v is pd.NA or pd.isna(v):
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v

with engine.begin() as conn:
    country_map = (
        pd.read_sql('SELECT "ID_Country", "Name" FROM public."Country"', conn)
          .assign(country_key=lambda d: d["Name"].apply(clean_country))
    )
    year_map = pd.read_sql('SELECT "ID_year", "year" FROM public."Year"', conn)


env = (
    pd.read_csv(PATH_ENV)
      .rename(columns={"Country": "country",
                       "Year": "year",
                       "CO2 Emission": "co2",     
                       "ELUC": "eluc"})
)
env = env[env["year"].between(YEAR_MIN, YEAR_MAX)].copy()
env["country_key"] = env["country"].apply(clean_country)
env["co2"]  = env["co2"].apply(parse_number)
env["eluc"] = env["eluc"].apply(parse_number)

df = (env
      .merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
      .merge(year_map, on="year", how="inner")
)

df_final = (df[["ID_Country", "ID_year", "co2", "eluc"]]
            .drop_duplicates(subset=["ID_Country", "ID_year"])
            .reset_index(drop=True)
)

update_sql = text("""
    UPDATE "Country_Year"
       SET "Environmental_ID" = :env_id          -- campo da Country_Year
     WHERE "Country_ID_Country" = :country_id
       AND "Year_ID_year"      = :year_id
       AND ("Environmental_ID" IS NULL OR "Environmental_ID" <> :env_id)
""")

with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        db_cols, params = [], {}

        if row.co2 is not None:
            db_cols.append('"CO2_Emision"')         
            params["co2"] = py_val(row.co2)
        if row.eluc is not None:
            db_cols.append('"ELUC"')
            params["eluc"] = py_val(row.eluc)

        if not db_cols:
            continue

        col_list     = ", ".join(db_cols)
        placeholders = ", ".join(f":{p}" for p in params)

        insert_sql = text(f"""
            INSERT INTO "Environmental Indicator" ({col_list})
            VALUES ({placeholders})
            RETURNING "ID_Environmental"
        """)
        new_env_id = conn.execute(insert_sql, params).scalar_one()

        conn.execute(update_sql, {
            "env_id"    : new_env_id,
            "country_id": int(row.ID_Country),
            "year_id"   : int(row.ID_year),
        })

print(f"{len(df_final)} registros inseridos em 'Environmental Indicator' "
      f"e vinculados na Country_Year ✔️")
