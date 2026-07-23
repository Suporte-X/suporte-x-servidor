# Suporte X - Operacao e Deploy Seguro (Web/Servidor)

Atualizado em: 2026-04-17

## Caminhos oficiais

- Repo Android oficial: `C:\Users\X-Not\AndroidStudioProjects\SuporteX`
- Repo Web/Servidor oficial: `C:\Users\X-Not\Workspaces\SuporteX\web-servidor`
- Alias Android em workspace: `C:\Users\X-Not\Workspaces\SuporteX\android-app` (junction para o repo Android oficial)

## Regra pratica de deploy no Render

- O Render publica a partir de `origin/main` de `Suporte-X/suporte-x-servidor`.
- Commits em branch `codex/*` so entram em deploy apos merge para `main`.

## Fluxo minimo recomendado

1. Confirmar repo e remoto:
   - `git remote -v`
   - `git status -sb`
2. Se a mudanca deve ir para producao agora, finalizar em `main`.
3. Enviar para o GitHub:
   - `git push origin main`
4. Conferir no Render se o deploy puxou o hash correto.

## Checklist anti-regressao antes de encerrar tarefa

- `main == origin/main` no repo web/servidor.
- Sem arquivos Android versionados neste repo.
- Sem arquivos web/servidor no repo Android.
- Sessao registrada via `codex-memory` no repo Android.

## Login técnico e Turnstile

- O login técnico depende de `TECH_LOGIN_TURNSTILE_SITE_KEY` e `TECH_LOGIN_TURNSTILE_SECRET_KEY` no Render.
- O backend valida o token em `/api/auth/turnstile/verify`; a secret key nunca deve ir para o frontend, GitHub, Obsidian ou chat.
- Sem essas variáveis, o login técnico deve falhar fechado com proteção anti-bot indisponível.

## Privacidade e retenção

- Configurar no Render `PRIVACY_CONTACT_ENCRYPTION_KEY` com 32 bytes aleatórios em base64 ou hexadecimal. Sem a chave, o formulário público de exclusão falha fechado com `503` e não persiste o contato.
- Confirmar os TTLs de `account_deletion_operations`, `privacy_deletion_requests` e `legacy_webrtc_rooms`.
- Agendar pelo menos diariamente `npm run firestore:cleanup:execute` no diretório `server/`, mantendo monitoramento de falhas. Antes da primeira execução, revisar `npm run firestore:cleanup:dry`.
- Definir responsável e rotina diária para `npm run privacy:requests -- --status received`. A listagem não mostra contatos; a abertura exige claim explícito pelo operador e nunca deve ser redirecionada para logs.
- O job de retenção remove sessões e mídias; nunca executar `--execute` apontando para um projeto Firebase não conferido.
