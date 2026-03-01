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

- `SUPERVISOR_EMAIL`: e-mail do usuário que deve ser promovido automaticamente para supervisor.
- `SUPERVISOR_AUTO_BOOTSTRAP`: defina como `true` para rodar o bootstrap automático no startup.
- Projeto esperado: `suporte-x-19ae8`.

### Bootstrap automático do supervisor

Com `SUPERVISOR_AUTO_BOOTSTRAP=true`, o servidor tenta promover automaticamente o usuário de `SUPERVISOR_EMAIL` no startup:

- Busca o usuário com `getUserByEmail`.
- Mescla custom claims existentes com `role: "tech"` e `supervisor: true`.
- Garante/atualiza o documento `techs/{uid}` com perfil técnico ativo.

Para trocar o supervisor, basta alterar `SUPERVISOR_EMAIL` no Render e fazer redeploy.
