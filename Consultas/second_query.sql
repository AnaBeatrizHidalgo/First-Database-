WITH gen AS (
  SELECT  CPS."Country_ID_Country",
          CPS."Year_ID_year"            AS yr,
          SUM(CPS."Power_Generation")   AS total_twh
  FROM    "Country_Power Source" CPS
  GROUP BY 1,2
)
SELECT  C."Name"               AS country,
        Y.year,
        gen.total_twh,
        G."Investment_Energy"  AS invest_energy_usd,
        I."IDH",
        P."Renewable_Energy"   AS renewable_use_pct
FROM    gen
JOIN    "Country"  C  ON C."ID_Country" = gen."Country_ID_Country"
JOIN    "Year"     Y  ON Y.year         = gen.yr                  
JOIN    "Country_Year" CY
           ON CY."Country_ID_Country" = C."ID_Country"
          AND CY."Year_ID_year"       = Y."ID_year"                
JOIN    "GDP"            G  ON CY."GDP_ID"          = G."ID_GDP"
JOIN    "IDH"            I  ON CY."IDH_ID"          = I."ID_IDH"
JOIN    "Power Consumed" P  ON CY."ConsumePower_ID" = P."ID_Consumed"
WHERE   Y.year >= 2020
ORDER BY invest_energy_usd DESC, total_twh DESC;
