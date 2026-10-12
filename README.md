# ms-analytics-bff

BFF de analytics do ecossistema Zera. Um único programa Python, dois
entrypoints: API FastAPI e cronjob.

## Entrypoints

| Entrypoint | Módulo | Como rodar |
|---|---|---|
| API | `app.api.main` | `make run` / `make dev` |
| Cronjob | `app.cron.main` | `make cron` |

Health da API: `GET /health`.

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
