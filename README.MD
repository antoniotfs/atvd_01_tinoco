<div align="center">

# 🚀 NexusData Pipeline
**Arquitetura Moderna de Ingestão de Dados**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Fast-yellow.svg)](https://duckdb.org/)
[![Pandas](https://img.shields.io/badge/Pandas-Data_Analysis-150458.svg)](https://pandas.pydata.org/)
[![DataOps](https://img.shields.io/badge/DataOps-Ready-brightgreen.svg)]()

</div>

---

## 📌 Visão Geral

O **NexusData Pipeline** é a primeira iteração de um pipeline de dados analítico totalmente funcional. O objetivo prático é consolidar conhecimentos em **DataOps**, construindo um pipeline que consome dados brutos em formato `.csv`, processa-os, aplica tipagem rigorosa e os armazena em um motor OLAP local e ultrarrápido (**DuckDB**).

> **O Desafio**: Diariamente, dezenas de transações e pedidos caem na nossa *Landing Zone*. Como processamos isso de forma confiável e rápida para as equipes de negócio?

## 🛠️ Stack Tecnológico

| Tecnologia | Papel no Pipeline |
| :--- | :--- |
| 🐍 **Python** | Orquestração do pipeline, validações e transformações básicas. |
| 🐼 **Pandas** | Extração e leitura inteligente dos arquivos em memória. |
| 🦆 **DuckDB** | Nosso *Data Warehouse* embutido. Armazenamento colunar robusto. |
| 🐙 **Git** | Controle de versão essencial para práticas de *Rollback* e CI/CD. |

---

## 📂 Arquitetura do Repositório

```text
atvd_01/
├── data/
│   ├── raw/                # Dados brutos recebidos (CSV)
│   └── curated/            # (Futuro) Dados limpos e transformados
├── ingestion/
│   └── load_orders.py      # Script principal de ETL
├── warehouse/
│   └── local.duckdb        # Base de dados local (gerada na execução)
├── .gitignore              # Arquivos ignorados pelo Git (.venv, .duckdb)
└── README.md
```

## 🚀 Como Executar Localmente

### 1️⃣ Clonar o Repositório
```bash
git clone https://github.com/antoniotfs/atvd_01.git
cd atvd_01
```

### 2️⃣ Preparar o Ambiente Virtual
Garanta o isolamento das dependências criando um ambiente:
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate
```

### 3️⃣ Instalação e Execução
```bash
pip install pandas duckdb
python ingestion/load_orders.py
```

---

## 🧠 Q&A: Conceitos Aplicados de DataOps

<details>
<summary><b>1. Aonde habitam os Dados Brutos (Raw)?</b></summary>
Nossa <i>Landing Zone</i> (ou camada Bronze) está localizada em `data/raw/orders_2026_03_23.csv`. É onde a ingestão primária ocorre sem qualquer mutação.
</details>

<details>
<summary><b>2. O que o Orquestrador (Python) faz exatamente?</b></summary>
O script `load_orders.py` tem múltiplas responsabilidades:
- Verifica a integridade da origem.
- Carrega e converte (`Pandas`).
- Conecta ao motor Analytics (`DuckDB`).
- Executa DDL/DML (`CAST(coluna AS TIPO)`).
- Emite relatórios de log.
</details>

<details>
<summary><b>3. Fragilidade a Mudanças de Schema</b></summary>
Se renomearmos uma coluna na origem, o script falhará pois depende de declarações SQL estáticas. Isso evidencia a urgência da camada de <b>Contratos de Dados (Data Contracts)</b> para travar quebras não-esperadas.
</details>

<details>
<summary><b>4. E se algo der errado (Rollback)?</b></summary>
Tendo um versionamento no Git (Baseline), se injetarmos um bug crítico nas próximas implementações, o rollback para versões prévias torna-se seguro e auditável.
</details>

<details>
<summary><b>5. Esse pipeline está "Production-Ready"?</b></summary>
Ainda não. Falta maturidade em:
- <b>Testes de Dados:</b> (Checar nulos, valores negativos).
- <b>Observabilidade:</b> Alertas, logs sistematizados (DataDog, CloudWatch).
- <b>Airflow/Mage:</b> Agendamento das *runs* diárias.
- <b>Tratamento de Exceções.</b>
</details>

<br>
<div align="center">
  <i>Construído com ❤️ para o aprendizado de arquiteturas de dados modernas.</i>
</div>
