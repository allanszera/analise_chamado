import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

#CARREGA O CSV
caminho_csv = os.getenv("GOTO_CAMINHO")
df = pd.read_csv(caminho_csv)



#FILTRA TEMPO DE ESPERA >25
df = df[df['Wait time (s)']>=25] 

#ORDENA EM ORDEM DECRESCENTE PELO TEMPO DE ESPERA
df = df.sort_values(by=['Wait time (s)'], ascending=False)


#FILTRA APENAS ABANDONADA
df = df[df['Outcome'] == 'abandoned']


# Exibir todas as colunas do DataFrame filtrado
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)


#SALVA NO FORMATO PARQUET
df.to_parquet("data/goto_abandonada_25s", engine="pyarrow", index=False)