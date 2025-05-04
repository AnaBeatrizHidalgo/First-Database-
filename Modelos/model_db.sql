BEGIN;

-- ───────────────── TABLES ──────────────────

CREATE TABLE IF NOT EXISTS public."Country" (
    "ID_Country"  SERIAL PRIMARY KEY,
    "Name"        VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS public."Year" (
    "ID_year"  SERIAL PRIMARY KEY,
    year       INT UNIQUE          -- cada ano só aparece uma vez
);

CREATE TABLE IF NOT EXISTS public."Power Source" (
    "ID_Power"  SERIAL PRIMARY KEY,
    "Name"      VARCHAR(100),
    "Renewable" BOOLEAN
);

CREATE TABLE IF NOT EXISTS public."Power Consumed" (
    "ID_Consumed"      SERIAL PRIMARY KEY,
    "GWH"              NUMERIC(12,2),
    "PowerImport"      NUMERIC(12,2),
    "Renewable_Energy" NUMERIC(7,4)
);

CREATE TABLE IF NOT EXISTS public."Environmental Indicator" (
    "ID_Environmental" SERIAL PRIMARY KEY,
    "ELUC"             NUMERIC(15,10),
    "CO2_Emision"      NUMERIC(15,10)
);

CREATE TABLE IF NOT EXISTS public."GDP" (
    "ID_GDP"             SERIAL PRIMARY KEY,
    "GDP"                NUMERIC(12,2),
    "Investment_Energy"  NUMERIC(15,2),
    "Health_Expenditure" NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS public."IDH" (
    "ID_IDH"          SERIAL PRIMARY KEY,
    "IDH"             NUMERIC(6,2),
    "Electricity"     NUMERIC(6,2),
    "Sanitation"      NUMERIC(6,2),
    "Health"          NUMERIC(6,2),
    "Standard_Living" NUMERIC(6,2)
);

CREATE TABLE IF NOT EXISTS public."Sector" (
    "ID_Sector" SERIAL PRIMARY KEY,
    "Name"      VARCHAR(100)
);

-- (global ou por país; mantendo Country_ID caso precise)
CREATE TABLE IF NOT EXISTS public."Sector_Year" (
    "Sector_ID_Sector"   INT REFERENCES public."Sector",
    "Year_ID_year"       INT REFERENCES public."Year",
    "CO2_Emission"       NUMERIC(12,2),
    PRIMARY KEY ("Sector_ID_Sector","Year_ID_year")
);

CREATE TABLE IF NOT EXISTS public."Country_Power Source" (
    "Country_ID_Country" INT REFERENCES public."Country",
    "Power Source_ID_Power" INT REFERENCES public."Power Source",
    "Year_ID_year"       INT REFERENCES public."Year",
    "Power_Generation"   NUMERIC(12,2),
    PRIMARY KEY ("Country_ID_Country","Power Source_ID_Power","Year_ID_year")
);

CREATE TABLE IF NOT EXISTS public."Country_Year" (
    "Country_ID_Country" INT REFERENCES public."Country",
    "Year_ID_year"       INT REFERENCES public."Year",
    "GDP_ID"             INT REFERENCES public."GDP",
    "IDH_ID"             INT REFERENCES public."IDH",
    "Environmental_ID"   INT REFERENCES public."Environmental Indicator",
    "ConsumePower_ID"    INT REFERENCES public."Power Consumed",
    "Population"         INT,
    PRIMARY KEY ("Country_ID_Country","Year_ID_year")
);

-- ──────────────── SEEDS & UTILITIES ────────────────

-- Popular anos (2000..ano atual)
INSERT INTO public."Year"(year)
SELECT y
FROM generate_series(2000, EXTRACT(YEAR FROM CURRENT_DATE)::INT) y
ON CONFLICT DO NOTHING;

-- Gerar linhas vazias em Country_Year (facilita FK posteriores)
INSERT INTO public."Country_Year" ("Country_ID_Country","Year_ID_year")
SELECT c."ID_Country", y."ID_year"
FROM   public."Country" c
CROSS  JOIN public."Year" y
WHERE  y.year BETWEEN 2000 AND 2023
ON CONFLICT DO NOTHING;

INSERT INTO public."Power Source" ("Name", "Renewable") VALUES
('Other renewables excluding bioenergy', true),
('Bioenergy', true),
('Solar', true),
('Wind', true),
('Hydro', true),
('Nuclear', false),
('Oil', false),
('Gas', false),
('Coal', false);

COMMIT;
