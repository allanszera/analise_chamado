import pandas as pd
from dotenv import load_dotenv
from datetime import date
import os


load_dotenv()

#carrega os dados
parquet = os.getenv("GOTO_PARQUET")
df = pd.read_parquet(parquet)

print(df.dtypes)

hoje = date.today()
print(hoje)  

df['Start time'] = pd.to_datetime(df['Start time'])

#extrai a data
df['Data'] = df['Start time'].dt.date

# Agrupa por contrato e data, contando as perdas
agrupado = df.groupby(['Dialed number name', 'Data']).size().reset_index(name='Perdas')


# Total por contrato
total_por_contrato = agrupado.groupby('Dialed number name')['Perdas'].sum()

# Print organizado das informações
for contrato in agrupado['Dialed number name'].unique():
    total = total_por_contrato[contrato]
    print(f"\nContrato: {contrato} (Total: {total} perdas)")
    
    dados_contrato = agrupado[agrupado['Dialed number name'] == contrato]
    for _, linha in dados_contrato.iterrows():
        print(f"  {linha['Data']} → {linha['Perdas']} perdas")