"""
Popula a tabela "Country_Power Source" com os dados de geração elétrica
contidos em electricity-prod-source-stacked.csv, usando o mesmo “modelo”
(pandas + SQLAlchemy + dotenv) que você adotou para a tabela Country.
"""
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

CSV_PATH = "Datasets/electricity-prod-source-stacked.csv"

with engine.begin() as conn:
    country_map = pd.read_sql(
        'SELECT "ID_Country", "Name" FROM "Country"', conn)
    power_map = pd.read_sql(
        'SELECT "ID_Power", "Name" FROM "Power Source"', conn)
    year_map = pd.read_sql(
        'SELECT "ID_year", "year" FROM "Year" '
        'WHERE "year" BETWEEN 2000 AND 2024', conn)

country_dict = dict(zip(country_map["Name"],   country_map["ID_Country"]))
power_dict   = dict(zip(power_map["Name"],     power_map["ID_Power"]))
year_dict    = dict(zip(year_map["year"],      year_map["ID_year"]))

raw = pd.read_csv(CSV_PATH)

rename_cols = {
    col: col.split("Electricity from ")[1].split(" -")[0]
               .title().replace("Bioenergy", "Biomass")
    for col in raw.columns if col.startswith("Electricity from")
}
raw = raw.rename(columns=rename_cols)

df = (
    raw.melt(id_vars=["Entity", "Code", "Year"],
             value_vars=list(rename_cols.values()),
             var_name="Power_Source",
             value_name="Power_Generation")
       .dropna(subset=["Power_Generation"])
)

df = df[df["Year"].between(2000, 2024)]

df["Country_ID_Country"]    = df["Entity"].map(country_dict)
df["Power Source_ID_Power"] = df["Power_Source"].map(power_dict)
df["Year_ID_year"]          = df["Year"].map(year_dict)

df = df.dropna(subset=["Country_ID_Country",
                       "Power Source_ID_Power",
                       "Year_ID_year"])

df_cp = (
    df[["Country_ID_Country",
        "Power Source_ID_Power",
        "Year_ID_year",
        "Power_Generation"]]
      .astype({"Power_Generation": float})
      .round(2)
      .drop_duplicates()
)

with engine.begin() as conn:
    df_cp.to_sql("tmp_cp", conn, if_exists="replace", index=False)

    insert_sql = """
    INSERT INTO "Country_Power Source"
        ("Country_ID_Country",
         "Power Source_ID_Power",
         "Year_ID_year",
         "Power_Generation")
    SELECT  "Country_ID_Country",
            "Power Source_ID_Power",
            "Year_ID_year",
            "Power_Generation"
    FROM    tmp_cp
    ON CONFLICT DO NOTHING;
    DROP TABLE tmp_cp;
    """
    conn.execute(text(insert_sql))

print('Tabela "Country_Power Source" populada com sucesso! 🚀')
