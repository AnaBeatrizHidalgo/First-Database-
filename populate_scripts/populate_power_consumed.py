import re, os
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

PATH_PWR = Path("Datasets/PowerConsumid.csv")
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

pwr = (
    pd.read_csv(PATH_PWR)
      .rename(columns={
          "Country" : "country",
          "Year"    : "year",
          "Energy imports, net (% of energy use)" :
              "power_import",
          "Renewable energy consumption (% of total final energy consumption)" :
              "renewable",
          "GWH" : "gwh"
      })
)

pwr = pwr[pwr["year"].between(YEAR_MIN, YEAR_MAX)].copy()
pwr["country_key"] = pwr["country"].apply(clean_country)
pwr["gwh"]           = pwr["gwh"].apply(parse_number)
pwr["power_import"]  = pwr["power_import"].apply(parse_number)   
pwr["renewable"]     = pwr["renewable"].apply(parse_number)


df = (pwr
      .merge(country_map[["ID_Country", "country_key"]], on="country_key", how="inner")
      .merge(year_map, on="year", how="inner")
)

df_final = (df[["ID_Country", "ID_year", "gwh", "power_import", "renewable"]]
            .drop_duplicates(subset=["ID_Country", "ID_year"])
            .reset_index(drop=True)
)

update_sql = text("""
    UPDATE "Country_Year"
       SET "ConsumePower_ID" = :pwr_id
     WHERE "Country_ID_Country" = :country_id
       AND "Year_ID_year"       = :year_id
       AND ("ConsumePower_ID" IS NULL OR "ConsumePower_ID" <> :pwr_id)
""")

with engine.begin() as conn:
    for row in df_final.itertuples(index=False):
        db_cols, params = [], {}

        if row.gwh is not None:
            db_cols.append('"GWH"')
            params["gwh"] = py_val(row.gwh)
        if row.power_import is not None:               
            db_cols.append('"PowerImport"')
            params["power_import"] = py_val(row.power_import)
        if row.renewable is not None:
            db_cols.append('"Renewable_Energy"')
            params["renewable"] = py_val(row.renewable)

        if not db_cols:
            continue

        col_list     = ", ".join(db_cols)
        placeholders = ", ".join(f":{p}" for p in params)

        insert_sql = text(f"""
            INSERT INTO "Power Consumed" ({col_list})
            VALUES ({placeholders})
            RETURNING "ID_Consumed"
        """)
        new_pwr_id = conn.execute(insert_sql, params).scalar_one()

        conn.execute(update_sql, {
            "pwr_id"    : new_pwr_id,
            "country_id": int(row.ID_Country),
            "year_id"   : int(row.ID_year),
        })

print(f"{len(df_final)} registros inseridos em 'Power Consumed' e vinculados na Country_Year ✔️")
