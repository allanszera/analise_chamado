# 📞 Análise de Chamadas Perdidas por Contrato

Este projeto tem como objetivo analisar chamadas perdidas em centrais de atendimento, agrupando os dados por contrato (`Queue`) e por data. O relatório final exibe o total de chamadas perdidas por contrato e a distribuição por dia.

---

## 📂 Estrutura do Projeto

```
.
├── data/                       # Arquivos de entrada e saída (ex: .parquet)
├── src/
│   └── extract.py              # Script principal de processamento e análise
├── .env                        # Variáveis de ambiente (ex: caminho do arquivo)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## ⚙️ Funcionalidades

- Leitura de dados no formato `.parquet`
- Filtro de chamadas com `Outcome = abandoned`
- Agrupamento por:
  - Data
  - Contrato (Queue)
- Cálculo do total de perdas por contrato
- Geração de relatório estruturado no console

---

## 🚀 Como executar

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/seu-repo.git
cd seu-repo
```

### 2. Crie o ambiente virtual

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.\.venv\Scriptsctivate   # Windows
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Defina o caminho do arquivo `.parquet` no `.env`

```
CSV_PATH=data/dados_filtrados_2025-05-15.parquet
```

### 5. Execute o script

```bash
python src/extract.py
```

---

## 🧪 Exemplo de Saída

```
Contrato: Suporte TI (Total: 19 perdas)
  2025-05-01 → 12 perdas
  2025-05-02 → 7 perdas

Contrato: Atendimento SAC (Total: 5 perdas)
  2025-05-01 → 5 perdas
```

---

## 📦 Dependências

- pandas
- pyarrow
- python-dotenv

---


