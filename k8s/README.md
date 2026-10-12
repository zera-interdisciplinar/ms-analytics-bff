# k8s — ms-analytics-bff

Mesma imagem Docker para os dois entrypoints. O `Deployment` sobe a API
(`python -m app.api.main`); o `CronJob` sobrescreve o comando para
`python -m app.cron.main`. O placeholder `IMAGE_TAG` nos manifests é
substituído pelo GitHub Actions no deploy (`sed`), igual aos outros
serviços do workspace.

## Entrypoints

| Recurso | Comando | Porta |
|---|---|---|
| `Deployment ms-analytics-bff` | `python -m app.api.main` (CMD da imagem) | 8000 (`/health`) |
| `CronJob ms-analytics-bff` | `python -m app.cron.main` | — (job one-shot) |

Schedule padrão do CronJob: `0 * * * *` (hora cheia, UTC). `concurrencyPolicy: Forbid`.

## Secrets

Nenhum Secret é referenciado pelos manifests hoje — `APP_ENV` / `APP_PORT` /
`APP_HOST` vão no YAML. Quando o serviço passar a precisar de credenciais,
crie `ms-analytics-bff-secrets` em cada namespace e aponte via `secretKeyRef`
(mesmo padrão de `ms-artificial-intelligence-core-secrets` / `scrapy-secrets`):

```sh
kubectl create secret generic ms-analytics-bff-secrets -n qa \
  --from-literal=EXAMPLE_KEY='<valor-qa>'

kubectl create secret generic ms-analytics-bff-secrets -n production \
  --from-literal=EXAMPLE_KEY='<valor-prod>'
```

Sem o Secret referenciado no Deployment, o pod sobe. Com `secretKeyRef`
obrigatório e o Secret ausente, o pod trava em `CreateContainerConfigError`.

## Ordem de deploy

1. Secrets (quando existirem).
2. `deployment-qa.yaml` + `service-qa.yaml` + `cronjob-qa.yaml` (qa) /
   `deployment.yaml` + `service.yaml` + `cronjob.yaml` (production).
   O workflow aplica os três.
3. Rota no `infra-gtw-kong` (`manifests/{qa,prod}/ms-analytics-bff.yaml`).
   Sem isso a API não entra no Kong. O CronJob não precisa de Ingress.

## CI/CD

Espelha os outros serviços Python do org (`ms-artificial-intelligence-core`):

| Workflow | Trigger | O que faz |
|---|---|---|
| `.github/workflows/ci.yml` | PR para `qa` ou `main` | pytest + coverage ≥ 80% + Codecov |
| `.github/workflows/deploy-qa.yml` | push na branch `qa` | build `:qa` + `:qa-<sha>`, apply no namespace `qa` |
| `.github/workflows/deploy-prod.yaml` | tag `v*.*.*` | build `:<tag>` + `:latest`, apply no namespace `production` |

Auth GCP: Workload Identity Federation (`zera-pool` / `github-provider`),
SA `github-actions-deploy@zera-509513.iam.gserviceaccount.com`, cluster
`zera-cluster` em `southamerica-east1`, registry
`southamerica-east1-docker.pkg.dev/zera-509513/repo/ms-analytics-bff`.

Antes do primeiro deploy:

1. Liberar este repositório na attribute condition do provider WIF
   (senão o `google-github-actions/auth` falha com permission denied).
2. Criar o secret de repositório `CODECOV_TOKEN` (o `ci.yml` usa).
3. Garantir que a SA de deploy tem `roles/container.developer` no cluster
   e push no Artifact Registry (já vale para os outros serviços).

ArgoCD neste org só gerencia `infra-gtw-kong`. O rollout do app é
`kubectl apply` direto no workflow, não GitOps. Se alguém criar um
`Application` ArgoCD apontando para `k8s/` com `targetRevision: HEAD`
antes de `k8s/` existir em `main`, o ArgoCD falha — use a branch que
já tem os manifests (`qa`) até o merge em `main`.
