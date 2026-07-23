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

- `SUPERVISOR_BOOTSTRAP_ENABLED`: use `true` somente durante o bootstrap inicial; qualquer outro valor mantém a rota indisponível.
- `SUPERVISOR_BOOTSTRAP_EMAIL`: e-mail verificado que pode receber o papel de supervisor.
- `SUPERVISOR_BOOTSTRAP_SECRET`: segredo forte e exclusivo exigido pela rota de bootstrap.
- `HEALTH_DEEP_SECRET`: segredo exclusivo enviado no header `x-health-secret` para autorizar `GET /health?deep=1` em produção.
- `META_APP_SECRET` (ou `WHATSAPP_APP_SECRET`): segredo do aplicativo Meta usado para validar obrigatoriamente a assinatura do webhook WhatsApp em produção.
- `ALLOW_UNAUTHENTICATED_DEEP_HEALTH=true`: exceção somente para desenvolvimento local; nunca configure no ambiente publicado.
- `ALLOW_UNSIGNED_META_WEBHOOK=true`: exceção somente para desenvolvimento local; nunca configure no ambiente publicado.
- `TECH_LOGIN_TURNSTILE_SITE_KEY`: site key pública do widget Cloudflare Turnstile do login técnico.
- `TECH_LOGIN_TURNSTILE_SECRET_KEY`: secret key do mesmo widget, usada apenas no backend para `POST /api/auth/turnstile/verify`.
- `TECH_LOGIN_TURNSTILE_ALLOWED_HOSTNAMES`: opcional, lista separada por vírgula. Padrão: `suportex.app,www.suportex.app,localhost,127.0.0.1`.
- `PRIVACY_CONTACT_ENCRYPTION_KEY`: chave aleatória de 32 bytes em base64 ou hexadecimal. É obrigatória para cifrar e aceitar contatos enviados pelo formulário público de exclusão; sem ela, somente essa rota responde `503` e nenhum contato é persistido.
- `CLOUDFLARE_TURN_KEY_ID`: ID da chave Cloudflare Realtime TURN, mantido somente no servidor.
- `CLOUDFLARE_TURN_KEY_API_TOKEN`: token da chave TURN, mantido somente no servidor e nunca enviado diretamente ao app ou painel.
- `CLOUDFLARE_TURN_TTL_SECONDS`: opcional, duração da credencial temporária entre 300 e 86400 segundos. Padrão: 3600.
- `CLOUDFLARE_TURN_TIMEOUT_MS`: opcional, timeout da Cloudflare entre 500 e 10000 ms. Padrão: 4000.
- `CLOUDFLARE_TURN_CACHE_SECONDS`: opcional, cache interno entre 1 e 60 segundos. Padrão: 30.
- Projeto esperado: `suporte-x-19ae8`.

`GET /api/webrtc/ice-config?sessionId=...` exige Bearer token e libera a configuração somente ao cliente ou técnico ativo vinculado à sessão. Sem configuração TURN ou em falha temporária da Cloudflare, a rota retorna STUN com `source` de contingência para não interromper a chamada.

## Privacidade e exclusão de conta

- `POST /api/client/account/delete`: exclusão autenticada, idempotente e vinculada ao UID. Contas com telefone verificado exigem uma prova PNV recente.
- `POST /api/privacy/deletion-requests`: pedido público protegido por Turnstile, com resposta genérica para não revelar se uma conta existe.
- `/privacidade` e `/excluir-conta`: páginas públicas canônicas usadas no aplicativo e no cadastro da Google Play.

O formulário público reutiliza as chaves `TECH_LOGIN_TURNSTILE_*`, mas envia a ação
`privacy_deletion_request`. O servidor valida hostname e ação antes de armazenar o pedido e cifra
o contato com AES-256-GCM usando `PRIVACY_CONTACT_ENCRYPTION_KEY`; o índice de busca do contato usa
HMAC-SHA-256 com a mesma chave, sem manter um hash simples enumerável. Gere uma chave nova sem
registrá-la no repositório:

```bash
node -e "console.log(require('node:crypto').randomBytes(32).toString('base64'))"
```

Guarde a chave no cofre do ambiente. Trocá-la sem antes concluir ou migrar os pedidos pendentes
impede a abertura dos contatos já cifrados.

### Triagem segura dos pedidos manuais

O script local usa Firebase Admin e a chave de cifragem. A listagem padrão mostra apenas metadados,
sem contato ou índice de busca:

```bash
npm run privacy:requests -- --status received
```

Para processar um pedido, use um identificador operacional estável e não pessoal. Primeiro assuma o
pedido; só o mesmo operador pode abrir o contato. Não redirecione a saída de `--reveal-contact` para
arquivo, monitoramento ou logs:

```bash
npm run privacy:requests -- --request-id <uuid> --set-status processing --actor operador-privacidade-1
npm run privacy:requests -- --request-id <uuid> --reveal-contact --actor operador-privacidade-1
npm run privacy:requests -- --request-id <uuid> --set-status completed --actor operador-privacidade-1
```

Ao concluir ou rejeitar, o script remove o contato cifrado e o índice do documento. O restante do
registro operacional expira por TTL e também é coberto pelo job de retenção. A exclusão efetiva da
conta continua exigindo confirmação de titularidade pelos canais oficiais.

Os documentos temporários usam o campo timestamp `expiresAt`. Ative TTL no projeto de produção:

```bash
gcloud firestore fields ttls update expiresAt --collection-group=account_deletion_operations --enable-ttl --project=suporte-x-19ae8
gcloud firestore fields ttls update expiresAt --collection-group=privacy_deletion_requests --enable-ttl --project=suporte-x-19ae8
gcloud firestore fields ttls update expiresAt --collection-group=legacy_webrtc_rooms --enable-ttl --project=suporte-x-19ae8
```

Depois, confirme o estado com:

```bash
gcloud firestore fields ttls list --project=suporte-x-19ae8
```

Não coloque tokens PNV, contatos ou credenciais em logs.

O prazo de retenção publicado depende da execução periódica do job abaixo. Agende-o pelo menos uma
vez ao dia no ambiente de produção, primeiro validando o dry-run. Ele remove recursivamente
`pnv_requests`, sessões encerradas, subcoleções, mídias em Storage, `support_sessions` finalizadas e
`support_reports`, além dos espelhos e marcadores finais de fila. O job também funciona como
contingência do TTL, removendo documentos temporários cujo `expiresAt` já venceu:

```bash
npm run firestore:cleanup:dry
npm run firestore:cleanup:execute
```

O modo padrão é sempre dry-run. O uso de `--execute` é obrigatório para excluir candidatos.


### Permissões necessárias no Firebase/GCP

Para o painel **Gerenciar Técnicos** funcionar (criar, editar, resetar senha e excluir), a service account usada no backend precisa ter permissão de escrita no Firebase Authentication.

No projeto `suporte-x-19ae8`, garanta no IAM pelo menos:

- `Firebase Authentication Admin` (`roles/firebaseauth.admin`)
- `Service Account Token Creator` (`roles/iam.serviceAccountTokenCreator`)

Sem essas roles, as rotas `/api/admin/create-tech` e `/api/admin/update-tech` retornam erro de permissão do Admin SDK.

### Bootstrap do supervisor

Para promover o e-mail definido em `SUPERVISOR_BOOTSTRAP_EMAIL`:

- Defina temporariamente `SUPERVISOR_BOOTSTRAP_ENABLED=true`.
- Faça login no painel técnico com esse usuário e confirme que o e-mail está verificado.
- Chame `POST /api/admin/bootstrap-supervisor` com Bearer token do usuário e body `{ "secret": "<SUPERVISOR_BOOTSTRAP_SECRET>" }`.
- A rota valida e grava `supervisor: true` nas custom claims desse UID.
- Após o sucesso, remova a variável de habilitação ou altere-a para `false`.
