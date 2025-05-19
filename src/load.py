import os
import requests
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

# Caminho para o arquivo storytelling gerado pelo transform.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(BASE_DIR, '..', 'data')
storytelling_path = os.path.join(data_dir, 'storytelling.txt')

# LEIA O TEXTO GERADO
with open(storytelling_path, 'r', encoding='utf-8') as f:
    storytelling = f.read()

# COLE AQUI O SEU WEBHOOK DO TEAMS
teams_webhook_url = os.getenv("API")

# MONTA O PAYLOAD
payload = {
    "text": storytelling
}

# ENVIA PARA O TEAMS
response = requests.post(teams_webhook_url, json=payload)

# FEEDBACK NO TERMINAL
if response.status_code in (200,202):
    print("✅ Mensagem enviada para o Teams com sucesso!")
else:
    print(f"❌ Falha ao enviar mensagem para o Teams: {response.status_code} {response.text}")
