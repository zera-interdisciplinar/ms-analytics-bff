# ms-analytics-bff

[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

BFF de analytics do sistema Zera: calcula os indicadores do dashboard a partir dos dados do
[`ms-administrative-core`](https://github.com/zera-interdisciplinar/ms-administrative-core)
(PostgreSQL) e do [`ms-inventory`](https://github.com/zera-interdisciplinar/ms-inventory) (Neo4j) e os
entrega ao painel web ([`zera-web-app`](https://github.com/zera-interdisciplinar/zera-web-app)) por
SSE. Um único programa Python com dois entrypoints: a API FastAPI e um cronjob.

## Sumário

- [Domínio](#domínio)
- [Arquitetura](#arquitetura)
- [Stack](#stack)
- [Executando localmente](#executando-localmente)
- [Configuração](#configuração)
- [API](#api)
- [Testes](#testes)
- [Deploy](#deploy)
- [Licença](#licença)

## Domínio

O cronjob executa o pipeline **medallion** em memória: a **Bronze** extrai os dados operacionais e
devolve DataFrames, a **Silver** trata esse lote e a **Gold** calcula os KPIs. No fim, o cron
**grava** o recorte analítico no PostgreSQL (`analytics.user_scope` e `analytics.unit_snapshot`),
sempre particionado por empresa e unidade. A API **só lê** esse lote —
`WHERE organization_id = ? AND unit_id = ?` — e empurra a visão geral por SSE. Cada execução do
pipeline carrega um `batch_id`, e as três camadas precisam concordar com ele.

A extração é **somente leitura** nas tabelas `public` do PostgreSQL e no Neo4j; a única escrita
do serviço é no schema `analytics`.

Os notebooks em `notebooks/` são a versão Databricks (Delta) do medallion. `app/pipeline/` é a
**mesma regra** em pandas para o cron: extração, projeções Silver e KPIs Gold precisam permanecer
alinhados nos dois lados.

## Arquitetura

```
app/
├── api/         FastAPI: rotas, validação do JWT, filtros do overview, SSE, leitura do warehouse analytics
├── cron/        Entrypoint do cronjob: roda o pipeline e grava o lote
└── pipeline/    Bronze → Silver → Gold, em pandas (sources.py acessa o PostgreSQL e o Neo4j)
config/          Leitura das variáveis de ambiente
logger/          Logger do serviço
notebooks/       Versão Databricks (Delta) do medallion
k8s/             Manifests do Deployment da API e do CronJob
```

## Stack

| | |
|---|---|
| Linguagem | **Python 3.12** |
| API | FastAPI + Uvicorn, SSE |
| Pipeline | pandas, numpy |
| Fontes de dados | PostgreSQL (`pg8000`) e Neo4j 5 |
| Auth | JWT RS256 (PyJWT), emitido pelo ms-administrative-core |
| Testes | pytest + pytest-cov |

## Executando localmente

Pré-requisitos: Python 3.12 e acesso ao PostgreSQL e ao Neo4j do ambiente (a extração precisa dos
dois).

```sh
cp .env.example .env     # e preencha — veja "Configuração"
python -m venv .venv
source .venv/bin/activate
make install
make test-cov
make run                 # API em :8000
```

Com Docker:

```sh
cp .env.example .env
make up          # API em :8000
make up-cron     # roda o cronjob uma vez e sai
```

| Entrypoint | Módulo | Como rodar |
|---|---|---|
| API | `app.api.main` | `make run` / `make dev` (com reload) |
| Cronjob | `app.cron.main` | `make cron` |

`make help` lista todos os alvos.

## Configuração

Toda configuração vem de variável de ambiente (ou de um `.env` na raiz). Referência completa em
[`.env.example`](.env.example) e em `config/environments.py`; os Secrets em QA/produção estão
descritos em [`k8s/README.md`](k8s/README.md).

### Obrigatórias

| Variável | Efeito |
|---|---|
| `PG_HOST`, `PG_PASSWORD` (+ `PG_PORT`, `PG_DATABASE`, `PG_USER`) | PostgreSQL do admin-core: origem da extração Bronze e destino do schema `analytics` |
| `NEO4J_URI`, `NEO4J_PASSWORD` (+ `NEO4J_USER`, `NEO4J_DATABASE`) | Neo4j do ms-inventory, origem da extração Bronze |
| `JWT_PUBLIC_KEY` **ou** `JWT_JWKS_URL` | Como validar os tokens do admin-core. Sem nenhum dos dois a API sobe, mas recusa todo token |
| `JWT_ISSUER` | Claim `iss` esperado (`ms-administrative-core-qa` / `ms-administrative-core-prod` nos clusters) |

### Opcionais

| Variável | Default | O quê |
|---|---|---|
| `CORS_ORIGINS` | vazio | Origens permitidas, separadas por vírgula |
| `SSE_POLL_SECONDS`, `SSE_HEARTBEAT_SECONDS` | `2`, `15` | Intervalo de leitura do lote e do heartbeat do SSE |
| `PIPELINE_MAX_ROWS` | `100000` | Teto de linhas por entidade; acima dele a extração falha em vez de truncar |
| `APP_ENV`, `APP_HOST`, `APP_PORT` | `DEV`, `0.0.0.0`, `8000` | Ambiente e endereço da API |

## API

| Rota | Auth | Faz |
|---|---|---|
| `GET /health` | Público | Liveness |
| `GET /api/v1/dashboard/overview` | JWT RS256 (`MANAGER`/`EMPLOYEE`) | SSE da visão geral |

O JWT é o mesmo do admin-core (`iss`, `sub`, `role`); tokens de serviço são recusados. A empresa
**não** vem no query string: o `sub` resolve o escopo gravado pelo cron. Toda leitura é
`organização + unidade` — a unidade do token, ou `X-Unit-Id` / `unit_id` se o gestor da mesma
empresa pedir outra. Operário fica na unidade dele, e recorte de outra empresa responde `400`.

Filtros aceitos no SSE: `period` (`30d`/`6m`/`12m`, padrão `6m`), `from`+`to`, `category`,
`status`, `unit_id`, `responsible`, `q` (id, nome ou material), `view`
(`todos`/`pendentes`/`categoria`).

Eventos: `snapshot` (payload), `unavailable` (o cron ainda não publicou) e `heartbeat` (comentário
SSE). O front pode mandar `Authorization: Bearer` (fetch) ou `access_token` na query
(EventSource).

## Testes

```sh
make test        # pytest (usa ./.venv)
make test-cov    # + cobertura, falha abaixo de 80 %
```

Não dependem de infraestrutura externa: PostgreSQL e Neo4j são substituídos por dublês, e
`test_notebooks.py` guarda o alinhamento entre `app/pipeline/` e os notebooks.

## Deploy

CI (`ci.yml`) roda em toda PR para `main`/`qa`: pytest, cobertura mínima de 80 % e Codecov. Deploy
para QA acontece no push na branch `qa`; produção, no push de uma tag `v*.*.*`. A mesma imagem
Docker serve os dois entrypoints: o `Deployment` sobe a API e o `CronJob` (de hora em hora, UTC)
sobrescreve o comando para o pipeline. Secrets, ordem de deploy e a rota no gateway
(`infra-gtw-kong`) estão em [`k8s/README.md`](k8s/README.md) — sempre a fonte da verdade para o que
precisa existir em cada ambiente antes do deploy.

## Licença

[MIT](LICENSE)
