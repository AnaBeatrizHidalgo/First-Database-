import pandas as pd

# Caminho para o arquivo original
input_path = './Datasets/yearly_full_release_long_format.csv'  # ajuste conforme o local do seu arquivo
output_path = './Datasets/PowerGenerationEmission.csv'

# Indicadores que você quer manter
indicadores_desejados = [
    "Power sector emissions",
    "Electricity generation"
]

indicadores_Variable = [
    "Bioenergy", "Coal", "Gas", "Hydro", "Nuclear", "Other Fossil", "Other Renewables", "Solar", "Wind"
]

indicadores_Subcategory= ["Fuel"]
indicadores_Unit= ["TWh"]

# Carregar o arquivo CSV original
df = pd.read_csv(input_path)

# Filtrar apenas as linhas com os indicadores desejados
df_filtrado = df[df["Category"].isin(indicadores_desejados)]
df_filtrado2 = df_filtrado[df_filtrado["Variable"].isin(indicadores_Variable)]
df_filtrado3 = df_filtrado2[df_filtrado2["Subcategory"].isin(indicadores_Subcategory)]
db_filtrado4 = df_filtrado3[df_filtrado3["Unit"].isin(indicadores_Unit)]


# Criar a lista de colunas desejadas
colunas_desejadas = ["Area", "Year", "Category", "Variable", "Value"] + [str(ano) for ano in range(2000, 2025)]

# Selecionar apenas as colunas desejadas (se existirem no DataFrame)
colunas_existentes = [col for col in colunas_desejadas if col in df_filtrado.columns]
df_resultado = db_filtrado4[colunas_existentes]

df_resultado.to_csv(output_path, index=False)
print("Sucesso")
