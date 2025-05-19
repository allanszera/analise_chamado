import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

# Carrega os dados
parquet = os.getenv("GOTO_PARQUET")
df = pd.read_parquet(parquet)

# Converte a coluna "Start time" para datetime (já está em ISO format)
df['Start time'] = pd.to_datetime(df['Start time'])

# Exemplo: filtrar por um único dia (ex: 2025-05-05)
#filtro_dia = date(2025, 5, 16)
#df = df[df['Start time'].dt.date == filtro_dia]

# Se quiser intervalo:
# data_inicio = pd.to_datetime("2025-05-05")
# data_fim = pd.to_datetime("2025-05-10")
# df = df[(df['Start time'] >= data_inicio) & (df['Start time'] <= data_fim)]

# Tradução manual dos meses
meses_pt = {
    'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Março',
    'April': 'Abril', 'May': 'Maio', 'June': 'Junho',
    'July': 'Julho', 'August': 'Agosto', 'September': 'Setembro',
    'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'
}

# Obter o mês e ano atuais
hoje = datetime.today()
mes_nome_en = hoje.strftime('%B')
mes_nome_pt = meses_pt[mes_nome_en]
mes_atual = hoje.month
ano_atual = hoje.year

# Filtrar apenas os dados do mês atual
df = df[
    (df['Start time'].dt.month == mes_atual) &
    (df['Start time'].dt.year == ano_atual)
]

# Criar a coluna 'pRazo' com base no tempo de espera
df['pRazo'] = df['Wait time (s)'].apply(lambda x: 'Dentro' if x <= 20 else 'Fora')

# Função para calcular as métricas por grupo
def calcular_métricas(grupo):
    entrantes = grupo['Left Queue Reason'].notna().sum()
    atendidas = (grupo['Left Queue Reason'] == 'answered').sum()
    abandonadas = (grupo['Left Queue Reason'] == 'abandon').sum()
    perc_abandono = (abandonadas / entrantes * 100) if entrantes > 0 else 0.0
    dentro = grupo[(grupo['Left Queue Reason'] == 'answered') & (grupo['pRazo'] == 'Dentro')].shape[0]
    fora = grupo[(grupo['Left Queue Reason'] == 'answered') & (grupo['pRazo'] == 'Fora')].shape[0]
    perc_dentro = (dentro / atendidas * 100) if atendidas > 0 else 0.0
    media_espera = grupo[grupo['Left Queue Reason'] == 'answered']['Wait time (s)'].mean()
    return pd.Series({
        'Entrantes': entrantes,
        'Atendidas': atendidas,
        'Abandonadas': abandonadas,
        '% Abandono': perc_abandono,
        'Dentro (≤20s)': dentro,
        'Fora (>20s)': fora,
        '% Dentro': perc_dentro,
        '20s (med)': round(media_espera, 2)
    })

# Agrupar e calcular
relatorio_final = df.groupby('Dialed number name', group_keys=False).apply(calcular_métricas).reset_index()

relatorio_final.rename(columns={'Dialed number name': 'Contrato'}, inplace=True)

# Forçar inteiros e formatar percentuais
colunas_inteiras = ['Entrantes', 'Atendidas', 'Abandonadas', 'Dentro (≤20s)', 'Fora (>20s)']
relatorio_final[colunas_inteiras] = relatorio_final[colunas_inteiras].astype(int)
relatorio_final['% Abandono'] = relatorio_final['% Abandono'].apply(lambda x: f'{x:.2f}%')
relatorio_final['% Dentro'] = relatorio_final['% Dentro'].apply(lambda x: f'{x:.2f}%')


# Obter mês atual formatado
mes_atual = datetime.today().strftime('%B de %Y')

# Mensagem de abertura
print(f"📊 Relatório de Desempenho - Mês de {mes_nome_pt} de {ano_atual}")
print("=" * 80)
print("Este relatório apresenta o desempenho das filas (Contratos) com base no tempo de espera dos atendimentos.")
print("As métricas abaixo mostram o total de chamadas recebidas, abandonadas, atendidas e sua performance em relação ao prazo de 20 segundos.")
print("\nLegenda:")
print("📌 Dentro (≤20s): Chamadas atendidas com tempo de espera até 20 segundos.")
print("📌 Fora  (>20s): Chamadas atendidas com tempo de espera acima de 20 segundos.")
print("📌 % Abandono: Proporção de chamadas que foram abandonadas antes do atendimento.")
print("📌 % Dentro: Proporção dos atendimentos realizados dentro do prazo de 20 segundos.")
print("📌 20s (med): Tempo médio de espera das chamadas atendidas.\n")
print("=" * 80)

# Exibir a tabela final
print(relatorio_final.to_string(index=False))
