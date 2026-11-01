# ms-analytics-bff

BFF de analytics do ecossistema Zera. Um único programa Python, dois
entrypoints: API FastAPI e cronjob.

O cronjob executa o pipeline medallion em memória: Bronze devolve
DataFrames, a Silver trata esse lote e a Gold calcula os KPIs. No fim
o cron **grava** o recorte analítico no PostgreSQL (`analytics.user_scope`
e `analytics.unit_snapshot`), sempre particionado por empresa e unidade.
A API **só lê** esse lote — `WHERE organization_id = ? AND unit_id = ?` —
e empurra a visão geral por SSE.

Os notebooks em `notebooks/` são a versão Databricks (Delta) do medallion.
`app/pipeline/` é a mesma regra em pandas para o cron. Extração, projeções
Silver e KPIs Gold precisam permanecer alinhados nos dois lados.

## Entrypoints

| Entrypoint | Módulo | Como rodar |
|---|---|---|
| API | `app.api.main` | `make run` / `make dev` |
| Cronjob | `app.cron.main` | `make cron` |

## API

| Rota | Auth | Faz |
|---|---|---|
| `GET /health` | público | liveness |
| `GET /api/v1/dashboard/overview` | JWT RS256 (`MANAGER`/`EMPLOYEE`) | SSE da visão geral |

O JWT é o mesmo do admin-core (`iss`, `sub`, `role`). A empresa **não** vem no query string: o `sub` resolve o escopo gravado pelo cron. Toda leitura é `organização + unidade` (unidade do token, ou `X-Unit-Id` / `unit_id` se o gestor da mesma empresa pedir outra). Operário fica na unidade dele. Recorte de outra empresa responde `400`.

Filtros aceitos no SSE: `period` (`30d`/`6m`/`12m`, padrão `6m`), `from`+`to`, `category`, `status`, `unit_id`, `responsible`, `q` (id, nome ou material), `view` (`todos`/`pendentes`/`categoria`). Eventos: `snapshot` (payload), `unavailable` (cron ainda não publicou), `heartbeat` (comentário SSE). O front pode mandar `Authorization: Bearer` (fetch) ou `access_token` na query (EventSource).

## Local

```sh
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
make install
make test-cov
make run
```

Docker:

```sh
cp .env.example .env
make up          # API em :8000
make up-cron     # roda o cronjob uma vez e sai
```

## Deploy

Padrão do org. Detalhes em [`k8s/README.md`](k8s/README.md).

- CI em PR para `qa` / `main`
- QA: push na branch `qa`
- Prod: tag `v*.*.*`
