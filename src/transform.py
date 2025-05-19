import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import os
import json

# Descobre o diretório base do script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(BASE_DIR, '..', 'data')  # Sobe um nível, entra em data

# Lê as metas dos arquivos JSON
with open(os.path.join(data_dir, 'metas_nivel_servico.json'), 'r', encoding='utf-8') as f:
    metas_nivel_servico = json.load(f)

with open(os.path.join(data_dir, 'metas_abandono.json'), 'r', encoding='utf-8') as f:
    metas_abandono = json.load(f)

with open(os.path.join(data_dir, 'metas_tempo_medio.json'), 'r', encoding='utf-8') as f:
    metas_tempo_medio = json.load(f)

# Carrega as variáveis de ambiente
load_dotenv()

# Carrega os dados do Parquet
parquet = os.getenv("GOTO_PARQUET")
df = pd.read_parquet(parquet)

# Converte a coluna "Start time" para datetime
df['Start time'] = pd.to_datetime(df['Start time'])

# Tradução dos meses para português
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

# Filtrar os dados do mês atual
df = df[
    (df['Start time'].dt.month == mes_atual) &
    (df['Start time'].dt.year == ano_atual)
]

# Criar coluna 'pRazo' com base no tempo de espera
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
def dataframe_para_html_table(df):
    html = "<table border='1' style='border-collapse:collapse;'>"
    # Cabeçalho
    html += "<tr>"
    for col in df.columns:
        html += f"<th style='padding:4px 8px;background:#e0e0e0'>{col}</th>"
    html += "</tr>"
    # Linhas
    for _, row in df.iterrows():
        html += "<tr>"
        for cell in row:
            html += f"<td style='padding:4px 8px'>{cell}</td>"
        html += "</tr>"
    html += "</table><br>"
    return html
# Agrupar e calcular
relatorio_final = df.groupby('Dialed number name', group_keys=False).apply(calcular_métricas).reset_index()

relatorio_final.rename(columns={'Dialed number name': 'Contrato'}, inplace=True)
relatorio_final['Contrato'] = relatorio_final['Contrato'].str.replace('HEPTA_', '', regex=False)

# Forçar inteiros e formatar percentuais
colunas_inteiras = ['Entrantes', 'Atendidas', 'Abandonadas', 'Dentro (≤20s)', 'Fora (>20s)']
relatorio_final[colunas_inteiras] = relatorio_final[colunas_inteiras].astype(int)
relatorio_final['% Abandono'] = relatorio_final['% Abandono'].apply(lambda x: f'{x:.2f}%')
relatorio_final['% Dentro'] = relatorio_final['% Dentro'].apply(lambda x: f'{x:.2f}%')

# Mensagem de abertura e legenda
mensagem_abertura = (
    f"📊 Relatório de Desempenho - Mês de {mes_nome_pt} de {ano_atual}<br>"
    + "=" * 80 + "<br>"
    "Este relatório apresenta o desempenho das filas (Contratos) com base no tempo de espera dos atendimentos.<br>"
    "As métricas abaixo mostram o total de chamadas recebidas, abandonadas, atendidas e sua performance em relação ao prazo de 20 segundos.<br><br>"
    "Legenda:<br>"
    "📌 Dentro (≤20s): Chamadas atendidas com tempo de espera até 20 segundos.<br>"
    "📌 Fora  (>20s): Chamadas atendidas com tempo de espera acima de 20 segundos.<br>"
    "📌 % Abandono: Proporção de chamadas que foram abandonadas antes do atendimento.<br>"
    "📌 % Dentro: Proporção dos atendimentos realizados dentro do prazo de 20 segundos.<br>"
    "📌 20s (med): Tempo médio de espera das chamadas atendidas.<br>"
    + "=" * 80 + "<br>"
)

# Exibir a tabela final no terminal
print(relatorio_final.to_string(index=False))

# Função para gerar o storytelling para o Teams (com <br> como quebra de linha)
def gerar_storytelling_apresentacao(relatorio, metas_servico, metas_abandono):
    texto = "========== Storytelling de Apresentação ==========<br><br>"
    texto += "🟢 = Meta atingida    🔴 = Meta não atingida<br><br>"
    for idx, row in relatorio.iterrows():
        contrato = row['Contrato']
        perc_dentro = float(row['% Dentro'].replace('%', ''))
        perc_abandono = float(row['% Abandono'].replace('%', ''))
        meta_servico = metas_servico.get(contrato)
        meta_abandono = metas_abandono.get(contrato)
        if meta_servico is not None:
            if perc_dentro >= meta_servico:
                emoji_servico = '🟢'
                texto_servico = f"Atingiu a meta de Nível de Serviço ({perc_dentro:.2f}%, meta ≥ {meta_servico}%)"
            else:
                emoji_servico = '🔴'
                texto_servico = f"Não atingiu a meta de Nível de Serviço ({perc_dentro:.2f}%, meta ≥ {meta_servico}%)"
        else:
            emoji_servico = '⚠️'
            texto_servico = 'Meta de Nível de Serviço não definida'
        if meta_abandono is not None:
            if perc_abandono <= meta_abandono:
                emoji_abandono = '🟢'
                texto_abandono = f"Atingiu a meta de Abandono ({perc_abandono:.2f}%, meta ≤ {meta_abandono}%)"
            else:
                emoji_abandono = '🔴'
                texto_abandono = f"Não atingiu a meta de Abandono ({perc_abandono:.2f}%, meta ≤ {meta_abandono}%)"
        else:
            emoji_abandono = '⚠️'
            texto_abandono = 'Meta de Abandono não definida'
        texto += f"Contrato: {contrato}<br>"
        texto += f"   {emoji_servico} {texto_servico}<br>"
        texto += f"   {emoji_abandono} {texto_abandono}<br><br>"
    return texto

# Gera a tabela em HTML
tabela_html = dataframe_para_html_table(relatorio_final)

storytelling = gerar_storytelling_apresentacao(relatorio_final, metas_nivel_servico, metas_abandono)

texto_final = mensagem_abertura + tabela_html + storytelling

with open(os.path.join(data_dir, 'storytelling.txt'), 'w', encoding='utf-8') as f:
    f.write(texto_final)
