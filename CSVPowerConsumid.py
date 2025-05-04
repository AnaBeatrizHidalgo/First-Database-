import pandas as pd

# Caminho para o arquivo original
input_path = './Datasets/Indicadores_Filtrados.csv'  # ajuste conforme o local do seu arquivo
output_path = './Datasets/PowerConsumid.csv'

# Indicadores que você quer manter
indicadores_desejados = [
    """Energy imports, net (% of energy use)""",
    "Renewable energy consumption (% of total final energy consumption)"
]

# Carregar o arquivo CSV original
df = pd.read_csv(input_path)

# Filtrar apenas as linhas com os indicadores desejados
df_filtrado = df[df["Indicator Name"].isin(indicadores_desejados)]

# Criar a lista de colunas desejadas
colunas_desejadas = ["Country", "Indicator Name"] + [str(ano) for ano in range(2000, 2025)]

# Selecionar apenas as colunas desejadas (se existirem no DataFrame)
colunas_existentes = [col for col in colunas_desejadas if col in df_filtrado.columns]
df_resultado = df_filtrado[colunas_existentes]

df_long = df_resultado.melt(
    var_name='Year',
    id_vars=['Country', 'Indicator Name'],
    value_name='Value' 
)

# Pivotear a tabela para transformar a coluna Category em colunas separadas
df_powerinfo = df_long.pivot_table(
    index=["Year", "Country"],
    columns='Indicator Name',
    values='Value',
    aggfunc='first'  # assumindo que não há duplicatas
).reset_index()

# Renomear as colunas para remover o nome 'Category'
df_powerinfo.columns.name = None


#Para o segundo csv com dados para a tabela Power Consumied
df2 = pd.read_csv('./Datasets/yearly_full_release_long_format.csv')

indicadores_desejados2 = [
    "Electricity demand"
]

# Filtrar apenas as linhas com os indicadores desejados
df_filtrado2 = df2[df2["Category"].isin(indicadores_desejados2)]

# Criar a lista de colunas desejadas
colunas_desejadas = ["Country", "Year", "Value"]

# Selecionar apenas as colunas desejadas (se existirem no DataFrame)
colunas_existentes = [col for col in colunas_desejadas if col in df_filtrado2.columns]
df_consumid = df_filtrado2[colunas_existentes]


#Para ter compabilidade de dados
df_powerinfo['Year'] = df_powerinfo['Year'].astype(int)
df_consumid['Year'] = df_consumid['Year'].astype(int)

df_merged = pd.merge(
    df_powerinfo,
    df_consumid,
    on=['Year', 'Country'],  # Chaves de junção
    how='outer'          # Mantém todas as linhas de ambos os CSVs
)

df_merged.to_csv(output_path, index=False)
print("Sucesso")
