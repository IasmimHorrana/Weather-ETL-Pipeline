# 🌦️ Weather ETL Pipeline — Salvador

Pipeline de dados climáticos em tempo real orquestrado pelo **Apache Airflow**. Coleta dados da [OpenWeather API](https://openweathermap.org/api) a cada hora, persiste o JSON bruto no **MinIO** (Bronze), transforma e enriquece os dados (Silver), carrega no **PostgreSQL** com views analíticas (Gold) e dispara alertas automáticos via **Telegram**.

> **Escopo atual:** Fases 1 a 6 concluídas. Arquitetura Medallion completa (MinIO → PostgreSQL + views Gold), módulo de Alertas operacional, pipeline orquestrado pelo Airflow e Dashboard Analítico no Metabase. Próximo passo: CI/CD com GitHub Actions.

---

## 🎯 Objetivo do Projeto

Monitorar condições climáticas de Salvador com foco em:

- Coleta **automatizada e horária** via API (OpenWeatherMap) — orquestrada pelo Airflow
- Armazenamento bruto imutável (Bronze / Landing Zone no MinIO)
- Transformação, normalização e regras de negócio (Silver)
- Persistência histórica e views analíticas no PostgreSQL (Gold)
- Identificação de padrões e geração de alertas (chuva intensa, ventos fortes, umidade crítica)
- Treinamento prático de infra com Docker, pipelines ETL em camadas (Medallion) e qualidade de código

---

## 🏗️ Arquitetura

```
                    ┌─────────────────────────────┐
                    │  Apache Airflow (@hourly)    │
                    │  Scheduler + Webserver       │
                    └──────────────┬──────────────┘
                                   │ orquestra
                                   ▼
OpenWeather API
      │
      ▼
  extract.py          ← Requisição HTTP + validação + retry (Tenacity)
      │
      ├──► MinIO (Bronze)    ← JSON bruto / imutável
      │    └── weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json
      │
      ├──► transform.py      ← Limpeza, tipagem e regras de negócio → MinIO (Silver)
      │
      ├──► load.py           ← PostgreSQL (tb_weather_history) — idempotente
      │
      ├──► gold.py           ← Aplica views analíticas no PostgreSQL (camada Gold)
      │
      └──► alertas.py        ← Notificações Telegram com retry (ALERTA / CRÍTICO)
```

### Camadas de dados (Medallion Architecture)

| Camada | Destino | Conteúdo |
|--------|---------|----------|
| **Bronze** | MinIO (`bronze`)     | JSON bruto da API, imutável |
| **Silver** | MinIO (`silver`)     | DataFrame normalizado, tipado e com nível de risco |
| **Gold**   | PostgreSQL (`views`) | Views analíticas prontas para consumo (Metabase) |

---

## 📦 Stack

### Infraestrutura (Docker)

| Serviço | Imagem | Porta | Função |
|---------|--------|-------|--------|
| **MinIO** | `minio/minio:latest` | `9000` / `9001` | Object storage S3-compatível (Bronze e Silver) |
| **PostgreSQL** | `postgres:15-alpine` | `5432` | Banco de dados principal (Gold / Histórico + metadata Airflow) |
| **pgAdmin** | `dpage/pgadmin4` | `5050` | Interface web para o PostgreSQL |
| **Metabase** | `metabase/metabase:latest` | `3000` | Ferramenta de BI para dashboards usando a camada Gold |
| **Airflow Webserver** | `infra-airflow` (custom) | `8080` | UI de monitoramento e controle das DAGs |
| **Airflow Scheduler** | `infra-airflow` (custom) | — | Motor de agendamento e execução (LocalExecutor) |
| **Airflow Init** | `infra-airflow` (custom) | — | Serviço efêmero: migra o banco e cria o usuário admin |

### Python

| Biblioteca | Versão mínima | Uso |
|------------|---------------|-----|
| `requests` | 2.33.1 | Requisições HTTP para a OpenWeather API |
| `boto3` | 1.42.96 | Cliente S3 para comunicação com o MinIO |
| `python-dotenv` | 1.2.2 | Carregamento das variáveis de ambiente do `.env` |
| `pandas` | 2.1.4 | Transformação e normalização dos dados (Silver) |
| `sqlalchemy` | 2.0.49 | ORM / conexão com o PostgreSQL |
| `psycopg2-binary` | 2.9.12 | Driver PostgreSQL para o SQLAlchemy |
| `tenacity` | 9.1.4 | Retries automáticos (alertas e requisições HTTP) |

### Qualidade de código (dev)

| Ferramenta | Função |
|------------|--------|
| `ruff` | Linter + formatter (substitui flake8, isort, black) |
| `mypy` | Verificação de tipos estáticos |
| `pytest` | Testes automatizados |

### Gerenciador de pacotes

- **uv** — gerenciador moderno e ultrarrápido para projetos Python

---

## ⚙️ Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e rodando
- [Python 3.12+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) instalado
- Chave de API gratuita do [OpenWeatherMap](https://home.openweathermap.org/users/sign_up)
- (Opcional) Bot do Telegram para receber alertas meteorológicos

---

## 🚀 Como executar (passo a passo)

### 1. Clone o repositório

```bash
git clone https://github.com/IasmimHorrana/Weather-ETL-Pipeline.git
cd Weather-ETL-Pipeline
```

### 2. Configure as variáveis de ambiente

Copie o arquivo de exemplo e preencha com suas credenciais:

```bash
cp config/.env.example config/.env
```

Abra `config/.env` e preencha:

```dotenv
# ── OpenWeather ──────────────────────────────────────────
API_KEY=sua_chave_aqui
OPENWEATHER_CITY=Salvador

# ── MinIO (Object Storage) ───────────────────────────────
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ENDPOINT=http://localhost:9000
MINIO_BRONZE_BUCKET=bronze

# ── PostgreSQL ───────────────────────────────────────────
POSTGRES_USER=weather_user
POSTGRES_PASSWORD=weather_pass
POSTGRES_DB=weather_db
DATABASE_URL=postgresql://weather_user:weather_pass@localhost:5432/weather_db

# ── Telegram (opcional — necessário para alertas) ────────
TELEGRAM_BOT_TOKEN=seu_token_aqui
TELEGRAM_CHAT_ID=seu_chat_id_aqui
```

> **Importante:** o arquivo `config/.env` está no `.gitignore` e nunca deve ser commitado.

### 3. Build e inicialização da infraestrutura

Na **primeira execução**, é necessário fazer o build da imagem customizada do Airflow:

```bash
# Build da imagem do Airflow com as libs do projeto
docker compose -f infra/docker-compose.yml build

# Sobe todos os serviços em segundo plano
docker compose -f infra/docker-compose.yml up -d
```

> O serviço `airflow-init` roda automaticamente na primeira vez, cria o banco interno do Airflow (`airflow_db`) e o usuário `admin`. Ele encerra sozinho após ~30 segundos — isso é esperado.

Verifique o status com:

```bash
docker compose -f infra/docker-compose.yml ps
```

Todos devem aparecer como `healthy` ou `running` (exceto o `airflow-init`, que deve aparecer como `Exited`).

### 4. Instale as dependências Python (modo desenvolvimento)

```bash
uv sync
```

### 5. Pipeline automático via Airflow (modo produção)

Com a infraestrutura rodando, o Airflow já agenda e executa o pipeline automaticamente **a cada hora**. Acesse a UI para monitorar:

**→ [http://localhost:8080](http://localhost:8080)** — login: `admin` / `admin`

O DAG `coleta_salvador` executa 5 tasks em sequência a cada hora:

| Task | O que faz |
|------|-----------|
| `extract_bronze` | Busca o clima de Salvador na API → salva JSON no MinIO (Bronze) |
| `transform_silver` | Normaliza, calcula nível de risco → salva no MinIO (Silver) |
| `load_historico` | Insere os dados no PostgreSQL (idempotente) |
| `apply_gold_views` | Recria as views analíticas no PostgreSQL |
| `dispara_alertas` | Verifica condições críticas → envia alerta no Telegram se necessário |

### 6. Execução manual dos módulos (modo desenvolvimento)

Para rodar etapas individualmente sem o Airflow:

> ⚠️ **Sempre rode da raiz do projeto** (`weather-api/`).

```bash
uv run python -m src.extract    # Extração → MinIO Bronze
uv run python -m src.transform  # Transformação → MinIO Silver
uv run python -m src.load       # Carga → PostgreSQL
uv run python -m src.gold       # Views analíticas → PostgreSQL
uv run python -m src.alertas    # Verificação de alertas → Telegram
```

---

## 🖥️ Acessando os serviços

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| **Metabase** | [http://localhost:3000](http://localhost:3000) | Usuário criado no primeiro acesso |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin123` |
| **pgAdmin** | [http://localhost:5050](http://localhost:5050) | `admin@admin.com` / `admin` |

### Verificando os dados no MinIO

1. Acesse [http://localhost:9001](http://localhost:9001)
2. Navegue até o bucket `bronze`
3. Os arquivos seguem o padrão: `weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json`

---

## 🗂️ Estrutura do projeto

```
weather-api/
├── .github/
│   └── workflows/
│       ├── ci.yml                      # Pipeline de CI (em desenvolvimento)
│       └── cd.yml                      # Pipeline de CD (em desenvolvimento)
├── config/
│   ├── .env                            # Variáveis de ambiente (não commitado)
│   └── .env.example                    # Template com todas as variáveis necessárias
├── dags/
│   └── dag_coleta_salvador.py          # DAG do Airflow — 5 tasks, @hourly, PythonOperator
├── infra/
│   ├── docker-compose.yml              # Stack completa: MinIO + Postgres + pgAdmin + Airflow + Metabase
│   ├── airflow/
│   │   └── Dockerfile                  # Imagem customizada Airflow 2.10.5 + libs do projeto
│   └── postgres/
│       ├── 00_init_airflow.sh          # Cria o airflow_db no primeiro boot do Postgres
│       ├── init.sql                    # Schema: tb_weather_history + índices + constraints
│       └── gold/                       # Views analíticas da camada Gold
│           ├── vw_condicao_atual.sql
│           ├── vw_resumo_diario.sql
│           ├── vw_tendencia_temperatura.sql
│           ├── vw_estatisticas_mensais.sql
│           └── vw_alertas_historico.sql
├── src/
│   ├── __init__.py
│   ├── extract.py                      # Extração da API → MinIO (Bronze)
│   ├── storage.py                      # Abstração do MinIO (upload/download/list)
│   ├── transform.py                    # Normalização e regras de negócio → MinIO (Silver)
│   ├── load.py                         # Carga idempotente no PostgreSQL
│   ├── gold.py                         # Aplica views analíticas no PostgreSQL (Gold)
│   └── alertas.py                      # Notificações Telegram com retry (Tenacity)
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   ├── test_gold.py
│   ├── test_storage.py
│   └── test_alertas.py
├── data/                               # JSONs locais (fallback de dev, no .gitignore)
├── notebooks/                          # Análise exploratória
├── conftest.py                         # Configuração e fixtures do pytest
└── pyproject.toml                      # Dependências e configuração das ferramentas
```

---

## 🧹 Qualidade de código

```bash
# Verificar erros de lint e tipo
uv run ruff check src/
uv run mypy src/

# Corrigir automaticamente + formatar
uv run ruff check --fix src/
uv run ruff format src/

# Rodar os testes
uv run pytest -v
```

---

## 🛑 Parando os serviços

```bash
# Para os contêineres (dados preservados)
docker compose -f infra/docker-compose.yml down

# Para os contêineres E apaga os volumes (dados perdidos — use com cuidado)
docker compose -f infra/docker-compose.yml down -v
```

> ⚠️ O flag `-v` apaga o histórico do banco e os arquivos do MinIO. Use apenas para reset completo do ambiente.

---

## 🗺️ Roadmap

- [x] Fase 1 — Extração da API e persistência no MinIO (Bronze)
- [x] Fase 2 — Transformação, regras de negócio e persistência no MinIO (Silver)
- [x] Fase 3 — Carga idempotente no PostgreSQL + views analíticas (Gold)
- [x] Fase 4 — Alertas automáticos via Telegram com retry (Tenacity)
- [x] Fase 6 — Orquestração do pipeline com Apache Airflow (coleta horária automatizada)
- [x] Fase 5 — Dashboard analítico no Metabase
- [ ] Fase 7 — CI/CD com GitHub Actions (testes automatizados e deploy)
