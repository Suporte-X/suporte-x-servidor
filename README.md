# Suporte X - Web/Servidor

Este repositorio contem somente o backend Web (Node.js + Firebase) e os assets da Central do Tecnico.

O app Android fica em repositorio separado:
- https://github.com/Suporte-X/suportex-android.git

Repositorio Web/Servidor oficial:
- https://github.com/Suporte-X/suporte-x-servidor.git

## Estrutura

```text
web-servidor/
|-- server/          # API e sinalizacao (Socket.IO)
|-- web/public/      # Front-end da Central e paginas web
|-- docs/            # Documentacao funcional e operacional
|-- firestore.rules  # Regras do Firestore
|-- storage.rules    # Regras do Storage
`-- firebase.json    # Configuracao Firebase
```

## Execucao local

```bash
npm install
npm run dev
```

Servidor local:
- http://localhost:3000

## Alertas de fila por WhatsApp (tecnicos)

Variaveis de ambiente esperadas no backend (`server/server.js`):

- `QUEUE_ALERTS_ENABLED` (padrao: `true`)
- `QUEUE_ALERT_SWEEP_INTERVAL_MS` (padrao: `60000`)
- `QUEUE_ALERT_FIRST_THRESHOLD_MINUTES` (padrao: `5`)
- `QUEUE_ALERT_STEP_MINUTES` (padrao: `5`)
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_API_VERSION` (padrao: `v21.0`)
- `WHATSAPP_QUEUE_ALERT_TEMPLATE_NAME` (padrao: `queue_wait_alert_v1`)
- `WHATSAPP_QUEUE_ALERT_TEMPLATE_LANGUAGE` (padrao: `pt_BR`)
- `WHATSAPP_QUEUE_ALERT_FORCE_TO` (opcional, somente para teste forcado de destinatario)

Template recomendado na Meta:
- Nome: `queue_wait_alert_v1`
- Idioma: `pt_BR`
- Variaveis do corpo (ordem): tecnico, minutos, cliente, requestId, aparelho, plataforma

## Observacoes de organizacao

- Nao colocar codigo Android neste repositorio.
- Nao colocar backend web no repositorio Android.
- Conteudo legado movido do repositorio Android foi arquivado em:
  - `docs/archive/android-repo-cleanup-2026-04-06/`
