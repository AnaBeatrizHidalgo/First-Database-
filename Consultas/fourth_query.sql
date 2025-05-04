SELECT  C."Name"                                            AS country,
        Y.year,
        EI."CO2_Emision"                     AS co2_mt,
        G."GDP"                              AS gdp_musd,
        ROUND( (EI."CO2_Emision" * 1000)
               / NULLIF(G."GDP", 0) , 4)     AS tco2_per_musd
FROM    "Country_Year"           CY
JOIN    "Country"                C  ON C."ID_Country"   = CY."Country_ID_Country"
JOIN    "Year"                   Y  ON Y."ID_year"      = CY."Year_ID_year"
LEFT JOIN "Environmental Indicator" EI ON EI."ID_Environmental" = CY."Environmental_ID"
LEFT JOIN "GDP"                   G  ON G."ID_GDP"      = CY."GDP_ID"
WHERE   EI."CO2_Emision" IS NOT NULL
  AND   G."GDP"          IS NOT NULL
ORDER BY tco2_per_musd DESC;
