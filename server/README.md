# Suporte X – Backend de Sinalização

Este diretório contém o servidor Node.js responsável por intermediar a sinalização entre cliente e técnico usando Socket.IO.

## Scripts

```bash
npm install   # instala dependências
npm start     # inicia em modo produção (PORT=3000 por padrão)
npm run dev   # alias para npm start
```

O servidor expõe os arquivos estáticos do painel a partir de `../web/public`.

## Variáveis de ambiente (Render)

Configure o Firebase Admin **sem** arquivo `serviceAccountKey.json` no repositório.

Opções suportadas:

1. `FIREBASE_SERVICE_ACCOUNT_JSON` com o JSON inteiro da service account.
2. Variáveis separadas: `FIREBASE_PROJECT_ID`, `FIREBASE_CLIENT_EMAIL`, `FIREBASE_PRIVATE_KEY`.

Também configure:

- `SUPERVISOR_BOOTSTRAP_SECRET`: segredo obrigatório para chamar `POST /api/admin/bootstrap-supervisor`.
- Projeto esperado: `suporte-x-19ae8`.


### Permissões necessárias no Firebase/GCP

Para o painel **Gerenciar Técnicos** funcionar (criar, editar, resetar senha e excluir), a service account usada no backend precisa ter permissão de escrita no Firebase Authentication.

No projeto `suporte-x-19ae8`, garanta no IAM pelo menos:

- `Firebase Authentication Admin` (`roles/firebaseauth.admin`)
- `Service Account Token Creator` (`roles/iam.serviceAccountTokenCreator`)

Sem essas roles, as rotas `/api/admin/create-tech` e `/api/admin/update-tech` retornam erro de permissão do Admin SDK.

### Bootstrap do supervisor

Para promover somente `isacxaviersoares@gmail.com`:

- Faça login no painel técnico com esse usuário.
- Chame `POST /api/admin/bootstrap-supervisor` com Bearer token do usuário e body `{ "secret": "<SUPERVISOR_BOOTSTRAP_SECRET>" }`.
- A rota valida e grava `supervisor: true` nas custom claims desse UID.
