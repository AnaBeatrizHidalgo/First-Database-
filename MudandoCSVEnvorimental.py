import pandas as pd

# 1. Carregar o CSV original
df = pd.read_csv('./Datasets/Dados CO2/EDGAR_2024_GHG_booklet_2024_fossilCO2only - fossil_CO2_totals_by_country.csv')

# Criar a lista de colunas desejadas
colunas_desejadas = ["Country Name", ] + [str(ano) for ano in range(2000, 2025)]

# Selecionar apenas as colunas desejadas (se existirem no DataFrame)
colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
df_resultado = df[colunas_existentes]

# 2. Transformar de wide para long usando melt()
df_co2 = df_resultado.melt(
    var_name='Year',
    id_vars=['Country'],
    value_name='CO2 Emission' 
)


#Para o segundo CSV com dados para a tabela Environmental Emission
df2 = pd.read_csv('./Datasets/Dados CO2/National_LandUseChange_Carbon_Emissions_2023v1.0 - H&C2023 (1).csv')

df_eluc = df2.melt(  
    var_name='Country',
    id_vars=['Year'],
    value_name='ELUC'
)


#Para ter compabilidade de dados
df_co2['Year'] = df_co2['Year'].astype(int)
df_eluc['Year'] = df_eluc['Year'].astype(int)

# 1. Juntar os DataFrames usando 'ano' e 'pais' como chaves
df_merged = pd.merge(
    df_co2,
    df_eluc,
    on=['Year', 'Country'],  # Chaves de junção
    how='outer'          # Mantém todas as linhas de ambos os CSVs
)

# 2. Salvar o CSV combinado
df_merged.to_csv('./Datasets/CombinandoEnviromental.csv', index=False)