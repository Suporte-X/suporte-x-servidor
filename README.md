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

## Observacoes de organizacao

- Nao colocar codigo Android neste repositorio.
- Nao colocar backend web no repositorio Android.
- Conteudo legado movido do repositorio Android foi arquivado em:
  - `docs/archive/android-repo-cleanup-2026-04-06/`
