# Firebase local setup

Objetivo: permitir que o Codex opere Firebase daqui do PC com o minimo possivel de painel manual.

## O que ja ficou pronto

- `firebase` CLI instalado no Windows
- `.firebaserc` apontando para `suporte-x-19ae8`
- `firebase.json` pronto para deploy de `firestore.rules` e `storage.rules`
- scripts locais para:
  - diagnostico: `npm run firebase:doctor`
  - ver contas logadas: `npm run firebase:whoami`
  - listar projetos: `npm run firebase:projects`
  - deploy de regras: `npm run firebase:deploy:rules`
  - criar tecnico: `npm run firebase:tech:create -- --email ... --password ... --name ...`
  - definir claim de tecnico: `npm run firebase:tech:claim -- <uid>`

## Melhor forma de liberar acesso para o Codex

Opcao recomendada: service account local.

1. No projeto `suporte-x-19ae8`, crie uma service account com acesso ao que voce quer automatizar.
2. Baixe o JSON.
3. Salve o arquivo em:

```text
C:\Users\X-Not\Workspaces\SuporteX\web-servidor\.secrets\firebase-admin.json
```

Com isso, os scripts locais ja passam a enxergar a credencial sem precisar editar codigo.

## Alternativa

Voce tambem pode rodar:

```bash
firebase login
```

Isso ajuda no CLI, mas para automacoes de Admin SDK a service account local continua sendo a melhor opcao.

## Permissoes recomendadas

Minimo util para o Suporte X:

- Firestore Rules Admin
- Storage Admin ou permissao equivalente para rules
- Firebase Authentication Admin
- Service Account Token Creator

## Comandos uteis

No diretorio `C:\Users\X-Not\Workspaces\SuporteX\web-servidor`:

```bash
npm run firebase:doctor
npm run firebase:deploy:rules
npm run firebase:projects
npm run firebase:tech:create -- --email tecnico@empresa.com --password SenhaTemp123! --name "Tecnico 1"
```

## O que o Codex ainda nao consegue pular sozinho

- login humano na conta Google
- criacao inicial da service account no painel Google/Firebase
- aceite de telas de consentimento da conta

Depois disso, quase todo o resto pode ser operado por comando local.
