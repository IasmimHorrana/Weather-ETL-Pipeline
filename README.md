# 🌦️ Weather ETL Pipeline — Salvador

Pipeline de dados climáticos em tempo real. Coleta dados da [OpenWeather API](https://openweathermap.org/api), persiste o JSON bruto no **MinIO** (camada Bronze) e prepara a infraestrutura para transformação, análise e geração de alertas meteorológicos para **Salvador, BA**.

> **Escopo atual:** Fases 1, 2, 3 e 4 concluídas. Arquitetura Medalhão completa (MinIO → Postgres) e módulo de Alertas pronto. Próximos passos: Metabase e Airflow.

---

## 🎯 Objetivo do Projeto

Monitorar condições climáticas de Salvador com foco em:

- Coleta automatizada via API (OpenWeatherMap)
- Armazenamento bruto imutável (Bronze / Landing Zone)
- Identificação de padrões e geração de alertas (chuva intensa, ventos fortes, umidade crítica)
- Treinamento prático de montagem de infra com Docker, pipelines ETL em camadas (Medallion) e qualidade de código

---

## 🏗️ Arquitetura

```
OpenWeather API
      │
      ▼
  extract.py          ← Requisição HTTP + validação
      │
      ├──► MinIO (Bronze)   ← JSON bruto / imutável
      │    └── weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json
      │
      ├──► transform.py     ← Limpeza e regras de negócio → MinIO (Silver)
      │
      ├──► load.py          ← PostgreSQL (tb_weather_history)
      │
      └──► alertas.py       ← Notificações Telegram (ALERTA / CRÍTICO)
```

### Camadas de dados (Medallion Architecture)

| Camada | Destino | Conteúdo |
|--------|---------|----------|
| **Bronze** | MinIO | JSON bruto da API, imutável |
| **Silver** | Disco / MinIO | DataFrame normalizado e tipado |
| **Gold** | PostgreSQL | Dados prontos para Metabase |

---

## 📦 Stack

### Infraestrutura (Docker)

| Serviço | Imagem | Porta | Função |
|---------|--------|-------|--------|
| **MinIO** | `minio/minio:latest` | `9000` / `9001` | Object storage S3-compatível (camada Bronze) |
| **PostgreSQL** | `postgres:15-alpine` | `5432` | Banco de dados principal |
| **pgAdmin** | `dpage/pgadmin4` | `5050` | Interface web para o PostgreSQL |

### Python

| Biblioteca | Versão mínima | Uso |
|------------|---------------|-----|
| `requests` | 2.33.1 | Requisições HTTP para a OpenWeather API |
| `boto3` | 1.42.96 | Cliente S3 para comunicação com o MinIO |
| `python-dotenv` | 1.2.2 | Carregamento das variáveis de ambiente do `.env` |
| `pandas` | 3.0.2 | Transformação e normalização dos dados (Silver) |
| `sqlalchemy` | 2.0.49 | ORM / conexão com o PostgreSQL |
| `psycopg2-binary` | 2.9.12 | Driver PostgreSQL para o SQLAlchemy |

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

---

## 🚀 Como executar (passo a passo)

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/weather-api.git
cd weather-api
```

### 2. Configure as variáveis de ambiente

Copie o arquivo de exemplo e preencha com suas credenciais:

```bash
cp config/.env.example config/.env
```

Abra `config/.env` e preencha:

```dotenv
# ── OpenWeather ──────────────────────────────────────────
OPENWEATHER_API_KEY=sua_chave_aqui
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
```

> **Importante:** o arquivo `config/.env` está no `.gitignore` e nunca deve ser commitado.

### 3. Suba a infraestrutura com Docker

```bash
docker compose -f infra/docker-compose.yml up -d
```

Aguarde os serviços ficarem saudáveis. Verifique com:

```bash
docker compose -f infra/docker-compose.yml ps
```

Todos devem aparecer com status `healthy` ou `running`.

### 4. Instale as dependências Python

```bash
uv sync
```

### 5. Execute a extração

```bash
uv run python -m src.extract
```

> ⚠️ **Sempre rode da raiz do projeto** (`weather-api/`). O import `from src.storage` exige que o Python enxergue a pasta `src/` como pacote — isso só acontece quando o CWD é a raiz.

O pipeline vai:
1. Ler a `API_KEY` do `config/.env`
2. Chamar a OpenWeather API para **Salvador, BR**
3. Salvar o JSON bruto em `data/weather_data.json` (fallback local)
4. Fazer upload para o MinIO em `weather-bronze/weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json`

### 6. Execute a transformação (Silver)

```bash
uv run python -m src.transform
```
> Busca automaticamente o arquivo mais recente da Bronze no MinIO, normaliza, aplica regras de risco e salva o resultado no bucket `silver`.

### 7. Execute a carga (Gold)

```bash
uv run python -m src.load
```
> Busca o arquivo mais recente da Silver no MinIO e faz a inserção (append) no PostgreSQL garantindo atomicidade.

### 8. Valide os Alertas

```bash
uv run python -m src.alertas
```
> Valida se há status `CRÍTICO` ou `ALERTA` e dispara mensagens via Telegram (requer credenciais no `.env`).

---

## 🖥️ Acessando os serviços

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **pgAdmin** | [http://localhost:5050](http://localhost:5050) | `admin@admin.com` / `admin` |

### Verificando os dados no MinIO

1. Acesse [http://localhost:9001](http://localhost:9001)
2. Faça login com as credenciais acima
3. Navegue até o bucket `bronze`
4. Os arquivos seguem o padrão: `weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json`

---

## 🗂️ Estrutura do projeto

```
weather-api/
├── config/
│   └── .env                    # Variáveis de ambiente (não commitado)
├── infra/
│   ├── docker-compose.yml      # MinIO + PostgreSQL + pgAdmin
│   └── postgres/
│       └── init.sql            # Schema inicial do banco
├── src/
│   ├── __init__.py
│   ├── extract.py              # Extração da API → MinIO (Bronze)
│   ├── storage.py              # Abstração do MinIO (upload/download/list)
│   ├── transform.py            # Normalização e regras de negócio (Silver)
│   ├── load.py                 # Carga no PostgreSQL (Gold)
│   └── alertas.py              # Notificações Telegram
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_alertas.py
├── data/                       # JSONs locais (fallback de dev, no .gitignore)
├── notebooks/                  # Análise exploratória
├── conftest.py                 # Configuração do pytest
└── pyproject.toml              # Dependências e configuração das ferramentas
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
uv run pytest
```

---

## 🛑 Parando os serviços

```bash
# Para os contêineres (dados preservados)
docker compose -f infra/docker-compose.yml down

# Para os contêineres E apaga os volumes (dados perdidos)
docker compose -f infra/docker-compose.yml down -v
```

---

## 🗺️ Roadmap

- [x] Fase 1 — Extração da API e persistência no MinIO (Bronze)
- [x] Fase 2 — Transformação, regras de negócio e persistência no MinIO (Silver)
- [x] Fase 3 — Carga atômica no banco relacional PostgreSQL (Gold / Histórico)
- [x] Fase 4 — Alertas automáticos via Telegram com controle de falhas (Tenacity)
- [ ] Fase 5 — Dashboard analítico no Metabase
- [ ] Fase 6 — Orquestração do pipeline completo com Apache Airflow


