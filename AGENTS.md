# SupportX Web - Integracao com Memoria

Este repositorio faz par com o repo Android e segue o mesmo protocolo de memoria.

## Inicio de tarefa (obrigatorio)

Executar o protocolo no repo Android:

```powershell
& C:\Users\X-Not\AndroidStudioProjects\SuporteX\tools\codex-memory.ps1 -Action start
```

## Encerramento tecnico (obrigatorio)

Registrar sessao no repo Android:

```powershell
& C:\Users\X-Not\AndroidStudioProjects\SuporteX\tools\codex-memory.ps1 -Action session -Text "<resumo curto da sessao>"
```

## Continuidade em outra maquina

- Os repositorios oficiais sao:
  - `https://github.com/Suporte-X/suportex-android.git`
  - `https://github.com/Suporte-X/suporte-x-servidor.git`
- Para preparar ambiente novo, executar no repo Android:

```powershell
& .\tools\bootstrap-recovery.ps1
```

## Regra de ouro de fronteira entre repositorios (obrigatoria)

- Aplicar separacao por dominio mesmo sem solicitacao explicita do usuario.
- Codigo Android/mobile deve permanecer no repo Android (`suportex-android`).
- Codigo Web/Servidor (HTML/JS/CSS, backend Node, regras Firebase, docs web) deve permanecer no repo Web/Servidor (`suporte-x-servidor`).
- Ao detectar arquivo em repositorio incorreto, mover para o repositorio correto antes de finalizar a tarefa.
- Antes de encerrar checkpoint em nuvem, confirmar estruturalmente:
  - repo Android: `main == origin/main` (0 ahead / 0 behind), remoto `suportex-android`;
  - repo Web/Servidor: `main == origin/main` (0 ahead / 0 behind), remoto `suporte-x-servidor`.

## Mapa operacional atual (informativo)

- Repo Android oficial: `C:\Users\X-Not\AndroidStudioProjects\SuporteX`
- Repo Web/Servidor oficial: `C:\Users\X-Not\Workspaces\SuporteX\web-servidor`
- Alias Android em workspace: `C:\Users\X-Not\Workspaces\SuporteX\android-app` (junction para o repo Android oficial)
- Deploy no Render: acompanhar `origin/main` do repo `suporte-x-servidor`
- Documento de referencia: `docs/operacao-e-deploy.md`
