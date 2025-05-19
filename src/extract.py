import pandas as pd
from dotenv import load_dotenv
import os

# === CONFIG ===
load_dotenv()
caminho_csv = os.getenv("GOTO_CAMINHO")
caminho_parquet = "data/goto_data_analytics.parquet"
chave_unica = 'Leg Id'

# === LER CSV NOVO ===
df_novo = pd.read_csv(caminho_csv)
df_novo['Prazo'] = df_novo['Wait time (s)'].apply(lambda x: 'Dentro' if x <= 20 else 'Fora')
df_novo = df_novo.sort_values(by=['Wait time (s)'], ascending=False)

# === CONTROLE DE DEDUPLICAÇÃO ===
linhas_novas = len(df_novo)
linhas_ignoradas = 0

if os.path.exists(caminho_parquet):
    df_existente = pd.read_parquet(caminho_parquet)
    linhas_anteriores = len(df_existente)

    # Juntar e deduplicar
    df_total = pd.concat([df_existente, df_novo], ignore_index=True)
    df_total.drop_duplicates(subset=chave_unica, keep='last', inplace=True)

    linhas_finais = len(df_total)
    linhas_ignoradas = (linhas_novas + linhas_anteriores) - linhas_finais
else:
    df_total = df_novo
    linhas_finais = len(df_total)

# === SALVAR PARQUET ===
df_total.to_parquet(caminho_parquet, engine="pyarrow", index=False)

# === RELATÓRIO DE EXECUÇÃO ===
print("✅ Parquet atualizado.")
print(f"➡️ Novas linhas no CSV: {linhas_novas}")
print(f"❌ Ignoradas por duplicidade: {linhas_ignoradas}")
print(f"📦 Total final no parquet: {linhas_finais}")
