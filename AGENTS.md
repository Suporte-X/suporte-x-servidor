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
