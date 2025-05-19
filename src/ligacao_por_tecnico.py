import pandas as pd
from dotenv import load_dotenv
from datetime import date
import os

load_dotenv()

FONTE_DADOS = os.getenv("GOTO_CAMINHO")

df = pd.read_csv(FONTE_DADOS)

print(df.dtypes)

df['Start time'] = pd.to_datetime(df['Start time'])

# Extrai a data
df['Data'] = df['Start time'].dt.date

# Extrai o nome do técnico
df['Name'] = df['Agent Name'].str.split(' - ').str[0]

# Contar por técnico e data
contagem_data = df.groupby(['Name', 'Data']).size().reset_index(name='Ligações')

# Contar total por técnico e ordenar decrescentemente
contagem_total = df['Name'].value_counts().reset_index()
contagem_total.columns = ['Name', 'total']
contagem_total = contagem_total.sort_values(by='total', ascending=False)

# Exibir no formato desejado
for _, row_total in contagem_total.iterrows():
    tecnico = row_total['Name']
    total = row_total['total']
    print(f"{tecnico} - Total: {total}")
    datas_tecnico = contagem_data[contagem_data['Name'] == tecnico]
    for _, row in datas_tecnico.iterrows():
        print(f"    {row['Data']} - {row['Ligações']}")
    print()
