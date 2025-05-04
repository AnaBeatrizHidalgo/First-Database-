SELECT  C."Name"            AS country,
        Y.year,
        I."Electricity"     AS electricity_index,
        P."Renewable_Energy" AS renewable_share_pct,
        GREATEST(P."PowerImport", 0)      AS import_gwh,
        GREATEST(-P."PowerImport", 0)     AS export_gwh,
        P."PowerImport"                   AS net_import_gwh
FROM    "Country_Year"  CY
JOIN    "Country"       C  ON C."ID_Country" = CY."Country_ID_Country"
JOIN    "Year"          Y  ON Y."ID_year"    = CY."Year_ID_year"
LEFT JOIN "IDH"         I  ON I."ID_IDH"     = CY."IDH_ID"
LEFT JOIN "Power Consumed" P ON P."ID_Consumed" = CY."ConsumePower_ID"
WHERE   I."Electricity" IS NOT NULL
ORDER BY electricity_index DESC, import_gwh DESC;
