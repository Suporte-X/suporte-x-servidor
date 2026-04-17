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
