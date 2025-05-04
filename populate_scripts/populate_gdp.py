import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_GDP = Path("../Datasets/country_year_gdp_energy_health.csv")  
YEAR_MIN, YEAR_MAX = 1960, 2025

def clean_country(val) -> str:
    if pd.isna(val):
        return ''
    return re.sub(r'\s+', ' ', str(val)).strip().casefold()

def parse_float(val):
    if pd.isna(val):
        return None
    try:
        return float(val)
    except ValueError:
        return None

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

df_src = (
    pd.read_csv(PATH_GDP)
      .rename(columns={
          "country_name": "country",
          "country_code": "code"          
      })
)

df_src["country_key"] = df_src["country"].apply(clean_country)
df_src["gdp"]               = df_src["gdp"].apply(parse_float)
df_src["investment_energy"] = df_src["investment_energy"].apply(parse_float)
df_src["health_expenditure"] = df_src["health_expenditure"].apply(parse_float)

df_src = df_src[df_src["year"].between(YEAR_MIN, YEAR_MAX)]

df = (df_src
      .merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
      .merge(year_map, left_on="year", right_on="year", how="inner")
)

df_final = (df[["ID_Country", "ID_year", "gdp",
                "investment_energy", "health_expenditure"]]
            .dropna(subset=["gdp"])                     
            .drop_duplicates(subset=["ID_Country", "ID_year"])
            .reset_index(drop=True)
)

update_sql = text("""
    UPDATE "Country_Year"
       SET "GDP_ID" = :gdp_id
     WHERE "Country_ID_Country" = :country_id
       AND "Year_ID_year"       = :year_id
       AND ("GDP_ID" IS NULL OR "GDP_ID" <> :gdp_id)
""")

with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        cols, params = [], {}

        cols.append('"GDP"')
        params["gdp"] = py_val(row.gdp)

        optional_cols = {
            "investment_energy" : '"Investment_Energy"',
            "health_expenditure": '"Health_Expenditure"',   
        }
        for attr, db_col in optional_cols.items():
            val = py_val(getattr(row, attr))
            if val is not None:
                cols.append(db_col)
                params[attr] = val

        col_list     = ", ".join(cols)
        placeholders = ", ".join(f":{p}" for p in params)

        insert_sql = text(f"""
            INSERT INTO "GDP" ({col_list})
            VALUES ({placeholders})
            RETURNING "ID_GDP"
        """)
        new_gdp_id = conn.execute(insert_sql, params).scalar_one()

        conn.execute(update_sql, {
            "gdp_id"    : new_gdp_id,
            "country_id": int(row.ID_Country),
            "year_id"   : int(row.ID_year),
        })

print(f"{len(df_final)} registros inseridos em 'GDP' e vinculados na Country_Year ✔️")
