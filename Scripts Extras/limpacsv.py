import pandas as pd

# Caminho para o arquivo original
input_path = './Datasets/yearly_full_release_long_format.csv'  # ajuste conforme o local do seu arquivo
output_path = './Datasets/EnergyData.csv'

# Indicadores que você quer manter
indicadores_desejados = [
    "Electricity generation",
    "Power sector emissions"
]

indicadores_desejados2 = [
    "Other renewables excluding bioenergy",
    "Bioenergy",
    "Solar",
    "Wind",
    "Hydro",
    "Nuclear",
    "Oil",
    "Gas",
    "Coal",
]

indicadores_desejados3 = [
    "TWh", "mtCO2"
]

# Carregar o arquivo CSV original
df = pd.read_csv(input_path)

# Filtrar apenas as linhas com os indicadores desejados
df_filtrado = df[df["Category"].isin(indicadores_desejados)]
df_filtrado2 = df_filtrado[df_filtrado["Variable"].isin(indicadores_desejados2)]
df_filtrado3 = df_filtrado2[df_filtrado2["Unit"].isin(indicadores_desejados3)]

# Criar a lista de colunas desejadas
colunas_desejadas = [
    "Country","Year","Category","Variable","Value"]

# Selecionar apenas as colunas desejadas (se existirem no DataFrame)
colunas_existentes = [col for col in colunas_desejadas if col in df_filtrado3.columns]
df_resultado = df_filtrado3[colunas_existentes]

# Pivotear a tabela para transformar a coluna Category em colunas separadas
pivoted_df = df_resultado.pivot_table(
    index=['Country', 'Year', 'Variable'],
    columns='Category',
    values='Value',
    aggfunc='first'  # assumindo que não há duplicatas
).reset_index()

# Renomear as colunas para remover o nome 'Category'
pivoted_df.columns.name = None

# Salvar o resultado em um novo arquivo CSV
pivoted_df.to_csv(output_path, index=False)

print("Novo arquivo CSV criado com sucesso:", output_path)
