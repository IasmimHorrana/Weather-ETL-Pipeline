# ⛈️ Monitoramento de Risco e Chuvas — Salvador ETL

Pipeline de engenharia de dados focado em **identificação de padrões climáticos críticos (chuvas fortes e vendavais)**. O sistema é orquestrado pelo **Apache Airflow**, coleta dados da [Open-Meteo API](https://open-meteo.com/) a cada hora, aplica a **Matriz de Risco do INMET** e dispara alertas automáticos via **Telegram** em caso de emergência climática.

> **Status do Projeto:** Todas as Fases (1 a 7) concluídas! O projeto conta com Arquitetura Medallion completa (MinIO → PostgreSQL), Orquestração (Airflow), Alertas (Telegram), Dashboard de BI (Metabase) e Pipeline de CI rigoroso com foco em qualidade.

---

## 🎯 Objetivo do Projeto

Ir além do simples "aplicativo de previsão do tempo" para criar um **monitoramento ativo de risco ambiental** em Salvador com foco em:

- **Alerta Precoce:** Identificação de condições climáticas críticas (matriz combinada de chuva/vento/umidade com base em parâmetros oficiais do Instituto Nacional de Meteorologia - INMET).
- **Orquestração e Escalabilidade:** Coleta automatizada e horária via API orquestrada nativamente pelo Apache Airflow.
- **Armazenamento Profissional (Medallion Architecture):**
  - **Bronze:** Armazenamento bruto e imutável no MinIO.
  - **Silver:** Transformação, limpeza, tipagem e cálculo do Nível de Risco.
  - **Gold:** Persistência no PostgreSQL otimizada com views semanais prontas.
- **Observabilidade Visual:** Dashboard dinâmico no Metabase exibindo tendências móveis e histórico semanal.
- **Qualidade Corporativa (CI/CD):** Garantia de código impecável com linting automático, tipagem estrita, cobertura de testes unitários e auditoria contra dependências vulneráveis (pip-audit).

---

## 🏗️ Arquitetura

```text
                    ┌─────────────────────────────┐
                    │  Apache Airflow (@hourly)   │
                    │  Scheduler + Webserver      │
                    └──────────────┬──────────────┘
                                   │ orquestra
                                   ▼
Open-Meteo API
      │
      ▼
  extract.py          ← Requisição HTTP + validação + retry (Tenacity)
      │
      ├──► MinIO (Bronze)    ← JSON bruto / imutável
      │    └── weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json
      │
      ├──► transform.py      ← Limpeza, tipagem e matriz de risco (INMET) → MinIO (Silver)
      │
      ├──► load.py           ← PostgreSQL (tb_weather_history) — inserção idempotente
      │
      ├──► gold.py           ← Atualiza views analíticas no PostgreSQL (camada Gold)
      │
      └──► alertas.py        ← Verifica risco (CRÍTICO/ALERTA) → Notifica no Telegram
```

### Camadas de dados (Medallion Architecture)

| Camada | Destino | Conteúdo |
|--------|---------|----------|
| **Bronze** | MinIO (`bronze`)     | JSON bruto da API, servindo como histórico imutável (Data Lake). |
| **Silver** | MinIO (`silver`)     | Dados achatados, padronizados, com tratamento de dados nulos e cálculo vetorial do risco. |
| **Gold**   | PostgreSQL (`views`) | Modelagem final em banco de dados relacional. Contém agregações temporais otimizadas para ingestão no Metabase. |

---

## 📦 Stack Tecnológica

### Infraestrutura Integrada (Docker Compose)

| Serviço | Função Principal |
|---------|------------------|
| **MinIO** | Object storage S3-compatível simulando um Data Lake. |
| **PostgreSQL** | Data Warehouse (Gold) + Metadados do Airflow. |
| **Metabase** | Plataforma de BI conectada diretamente à camada Gold. |
| **Airflow** | Motor de agendamento, dependência e execução das tarefas (DAGs). |

### Qualidade de Código & CI (GitHub Actions)

O projeto usa **uv** como gerenciador rápido de pacotes e aplica regras estritas a cada commit para assegurar o funcionamento da pipeline:
- **Ruff:** Formatação e Linter ultrarrápido configurado para padrões PEP-8.
- **MyPy Estrito:** Tipagem estática forçada (`disallow_untyped_defs = true`) prevenindo bugs silenciosos.
- **Pytest + pytest-cov:** Suíte de testes validando os extratores, matrizes matemáticas e conectores (com fail-under de cobertura de 70%).
- **Pip-Audit:** Auditoria automática contra dependências vulneráveis (segurança CVE).

---

## 🚀 Como executar (passo a passo)

### 1. Clone o repositório

```bash
git clone https://github.com/IasmimHorrana/Weather-ETL-Pipeline.git
cd Weather-ETL-Pipeline
```

### 2. Configure as variáveis de ambiente

```bash
cp config/.env.example config/.env
```
Preencha o arquivo `config/.env` com as credenciais do Telegram (Bot Token e Chat ID) para ativar o fluxo de alertas. (A API Open-Meteo é aberta e não exige chave de autenticação).

### 3. Suba a Infraestrutura Completa

```bash
# Faz o build da imagem customizada do Airflow e sobe todos os serviços
docker compose -f infra/docker-compose.yml up -d --build
```
*(Aguarde o serviço `airflow-init` inicializar o banco de dados interno. Pode levar de 30 a 60 segundos na primeira vez).*

### 4. Instale as bibliotecas localmente (para desenvolvimento)

```bash
uv sync --all-extras --dev
```

### 5. Monitorando em Produção (Airflow & Metabase)

Com os contêineres rodando, a automação já estará trabalhando por você.
- **Airflow:** [http://localhost:8080](http://localhost:8080) (user: `admin`, pass: `admin`). Ative a DAG `coleta_salvador` para agendar as buscas de chuva hora a hora.
- **Metabase:** [http://localhost:3000](http://localhost:3000). Configure o PostgreSQL como fonte de dados e crie o painel utilizando as tabelas `vw_gold_*`.
- **MinIO:** [http://localhost:9001](http://localhost:9001) (user: `minioadmin`, pass: `minioadmin123`). Visualize a ingestão progressiva do Data Lake.

---

## 🧹 Comandos Úteis de Desenvolvimento

```bash
# Validar tipagem restrita
uv run mypy src/ dags/

# Corrigir imports e lint
uv run ruff check --fix src/ tests/ dags/
uv run ruff format src/ tests/ dags/

# Testes com relatório de cobertura
uv run pytest -v --cov=src --cov-report=term-missing

# Auditoria de segurança
uv run pip-audit
```

---

## 🗺️ Roadmap de Evolução (Concluído)

- [x] **Fase 1:** Extração resiliente da API e Data Lake no MinIO (Bronze).
- [x] **Fase 2:** Engenharia de features, regras de negócio baseadas em Meteorologia e persistência (Silver).
- [x] **Fase 3:** Carga Data Warehouse idempotente no PostgreSQL + Views analíticas (Gold).
- [x] **Fase 4:** Inteligência ativa com Notificações Telegram e sistema de retentativas.
- [x] **Fase 5:** Dashboard analítico avançado no Metabase com séries temporais.
- [x] **Fase 6:** Automação e Orquestração robusta do pipeline inteiro no Apache Airflow.
- [x] **Fase 7:** CI/CD com GitHub Actions, tipagem rigorosa, cobertura de testes e auditoria de segurança.
