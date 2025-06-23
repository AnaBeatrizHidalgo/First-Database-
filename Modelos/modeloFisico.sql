BEGIN;

CREATE TABLE IF NOT EXISTS public."Country"
(
    "ID_Country" serial PRIMARY KEY,
    "Name" character varying(100)
);

CREATE TABLE IF NOT EXISTS public."Environmental Indicator"
(
    "ID_Environmental" serial PRIMARY KEY,
    "CO2_Emision" numeric(15, 10),
    "Year" integer,
    "Country_ID" integer NOT NULL,
    CONSTRAINT "fk_env_country" FOREIGN KEY ("Country_ID") REFERENCES public."Country" ("ID_Country")
);

CREATE TABLE IF NOT EXISTS public."Investment"
(
    "ID_Investment" serial PRIMARY KEY,
    "GDP" numeric(12, 2),
    "Investment_Energy" numeric(15, 2),
    "Health_Expenditure" numeric(15, 2),
    "Year" integer,
    "Country_ID" integer NOT NULL,
    CONSTRAINT "fk_inv_country" FOREIGN KEY ("Country_ID") REFERENCES public."Country" ("ID_Country")
);

CREATE TABLE IF NOT EXISTS public."Development"
(
    "ID_Development" serial PRIMARY KEY,
    "IDH" numeric(6, 2),
    "Electricity" numeric(6, 2),
    "Sanitation" numeric(6, 2),
    "Health" numeric(6, 2),
    "Standard_Living" numeric(6, 2),
    "Ano" integer,
    "Country_ID" integer NOT NULL,
    CONSTRAINT "fk_dev_country" FOREIGN KEY ("Country_ID") REFERENCES public."Country" ("ID_Country"),
    CONSTRAINT "uq_dev_country_ano" UNIQUE ("Country_ID", "Ano")
);

CREATE TABLE IF NOT EXISTS public."Power Consumed"
(
    "ID_Consumed" serial PRIMARY KEY,
    "GWH" numeric(12, 2),
    "PowerImport" numeric(12, 2),
    "Renewable_Energy" numeric(7, 4),
    "Year" integer,
    "Country_ID" integer NOT NULL,
    CONSTRAINT "fk_power_country" FOREIGN KEY ("Country_ID") REFERENCES public."Country" ("ID_Country"),
    CONSTRAINT "uq_power_country_year" UNIQUE ("Country_ID", "Year")
);

CREATE TABLE IF NOT EXISTS public."Power Source"
(
    "ID_Power" serial PRIMARY KEY,
    "Name" character varying(100),
    "Renewable" boolean
);

CREATE TABLE IF NOT EXISTS public."Sector"
(
    "ID_Sector" serial PRIMARY KEY,
    "Name" character varying(100)
);

CREATE TABLE IF NOT EXISTS public."Sector_Country"
(
    "Sector_ID_Sector" integer NOT NULL,
    "Country_ID_Country" integer NOT NULL,
    "CO2_Emission" numeric(12, 4),
    "Year" integer,
    CONSTRAINT "pk_sector_country" PRIMARY KEY ("Sector_ID_Sector", "Country_ID_Country", "Year"),
    CONSTRAINT "fk_sector_country_sector" FOREIGN KEY ("Sector_ID_Sector") REFERENCES public."Sector" ("ID_Sector"),
    CONSTRAINT "fk_sector_country_country" FOREIGN KEY ("Country_ID_Country") REFERENCES public."Country" ("ID_Country")
);

CREATE TABLE IF NOT EXISTS public."Power Source_Country"
(
    "Power Source_ID_Power" integer NOT NULL,
    "Country_ID_Country" integer NOT NULL,
    "Year" integer,
    "CO2_Emission" numeric(12, 4),
    "Power_Generation" numeric(12, 4),
    CONSTRAINT "pk_power_source_country" PRIMARY KEY ("Power Source_ID_Power", "Country_ID_Country"),
    CONSTRAINT "fk_psc_power" FOREIGN KEY ("Power Source_ID_Power") REFERENCES public."Power Source" ("ID_Power"),
    CONSTRAINT "fk_psc_country" FOREIGN KEY ("Country_ID_Country") REFERENCES public."Country" ("ID_Country")
);

CREATE TABLE IF NOT EXISTS public."Demography"
(
    "ID" serial PRIMARY KEY,
    "Year" integer,
    "Population" BIGINT,
    "Country_ID" integer NOT NULL,
    CONSTRAINT "fk_population_country" FOREIGN KEY ("Country_ID") REFERENCES public."Country" ("ID_Country"),
    CONSTRAINT "uq_population_country_year" UNIQUE ("Country_ID", "Year")
);

ALTER TABLE "Investment"
ADD CONSTRAINT uq_investment_country_year
UNIQUE ("Country_ID", "Year");

ALTER TABLE "Environmental Indicator"
ADD CONSTRAINT uq_environmental_country_year
UNIQUE ("Country_ID", "Year");


END;
