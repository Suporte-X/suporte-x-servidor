# Creditos e verificacao do cliente

## Confirmacao de creditos

Quando um tecnico adiciona creditos positivos pela ficha do cliente, o backend atualiza o saldo no Firestore e so depois dispara a confirmacao ao cliente por WhatsApp e e-mail.

Template WhatsApp aprovado esperado:

- Nome: `creditos_adicionados`
- Idioma: `pt_BR`
- Categoria: utilidade
- Variaveis do corpo, em ordem:
  - `{{1}}`: nome do cliente
  - `{{2}}`: quantidade adicionada
  - `{{3}}`: data
  - `{{4}}`: hora
  - `{{5}}`: quantidade adicionada
  - `{{6}}`: saldo atual

Variaveis de ambiente:

```text
WHATSAPP_ACCESS_TOKEN=...
WHATSAPP_PHONE_NUMBER_ID=...
WHATSAPP_API_VERSION=v21.0
CREDIT_ADDED_WHATSAPP_TEMPLATE_NAME=creditos_adicionados
CREDIT_ADDED_WHATSAPP_TEMPLATE_LANGUAGE=pt_BR
CREDIT_ADDED_WHATSAPP_TEMPLATE_USE_NAMED_PARAMS=false
RESEND_API_KEY=...
CREDIT_ADDED_EMAIL_FROM=Suporte X <no-reply@seudominio.com>
CREDIT_ADDED_EMAIL_REPLY_TO=suporte@seudominio.com
```

`CREDIT_ADDED_WHATSAPP_TEMPLATE_USE_NAMED_PARAMS` deve ficar `false` para modelo com variaveis numericas como `{{1}}`, `{{2}}`, etc. Se o template for recriado com parametros nomeados, configurar tambem:

```text
CREDIT_ADDED_WHATSAPP_TEMPLATE_BODY_PARAM_NAMES=nome_do_cliente,creditos_texto,data,hora,creditos_adicionados,saldo_atual
CREDIT_ADDED_WHATSAPP_TEMPLATE_USE_NAMED_PARAMS=true
```

## Quantidade personalizada

A ficha do cliente mantem os atalhos `+1`, `+3`, `+7`, `+10` e `-1`, e agora permite digitar uma quantidade personalizada entre 1 e 999 creditos.

## Verificacao manual por codigo

O painel gera um codigo de 6 digitos no servidor, salva apenas hash e salt em `client_verifications.manualCode`, e valida por 10 minutos.

Template WhatsApp esperado:

- Nome: `codigo_de_verificacao`
- Idioma: `pt_BR`
- Categoria: autenticacao
- Corpo: `Seu codigo de verificacao e {{1}}. Para sua seguranca, nao o compartilhe.`
- Botao: copiar codigo ou zero-tap, conforme configurado no Meta.

Variaveis:

```text
CLIENT_VERIFICATION_WHATSAPP_TEMPLATE_NAME=codigo_de_verificacao
CLIENT_VERIFICATION_WHATSAPP_TEMPLATE_LANGUAGE=pt_BR
CLIENT_VERIFICATION_WHATSAPP_TEMPLATE_USE_NAMED_PARAMS=false
CLIENT_VERIFICATION_WHATSAPP_COPY_CODE_BUTTON=true
CLIENT_MANUAL_VERIFICATION_CODE_TTL_MS=600000
CLIENT_MANUAL_VERIFICATION_MAX_ATTEMPTS=5
CLIENT_VERIFICATION_EMAIL_FROM=Suporte X <no-reply@seudominio.com>
CLIENT_VERIFICATION_EMAIL_REPLY_TO=suporte@seudominio.com
```

Observacao: o SMS atual do painel antigo usa Firebase Phone Auth e o Firebase nao permite definir manualmente o mesmo codigo enviado por WhatsApp/e-mail. Por isso, neste fluxo novo o mesmo codigo e enviado por WhatsApp e e-mail. Para SMS com o mesmo codigo, sera necessario configurar um provedor SMS transacional separado.

## Verificacao automatica por Firebase PNV

Quando o tecnico salva a ficha do cliente no painel, o backend cria uma tentativa em `pnv_requests` e envia `client:verification:trigger` para o app Android vinculado ao atendimento. O app executa `FirebasePhoneNumberVerification.getVerifiedPhoneNumber()`, recebe telefone + token assinado, e envia o token para:

```text
POST /api/client-context/verification/pnv-result
```

O servidor valida o token PNV com JWKS oficial do Firebase antes de marcar `client_verifications.status = verified`.

Configuracao esperada:

```text
FIREBASE_PROJECT_NUMBER=603259295557
```

Se a variavel nao existir, o servidor usa esse numero como fallback para o projeto `suporte-x-19ae8`.

## WhatsApp autenticacao: copy code, one-tap e zero-tap

Para o template `codigo_de_verificacao`, a opcao mais simples e mais robusta agora e `Copiar codigo`, porque o painel ja envia WhatsApp/e-mail e o tecnico digita o codigo recebido pelo cliente.

`Preenchimento automatico de zero toque` e `Preenchimento automatico de um toque` sao fluxos de app Android: o codigo chega ao app, nao ao painel web. Para usar esse fluxo de ponta a ponta, o app Android ainda precisa integrar o SDK/receiver OTP do WhatsApp e confirmar o codigo no backend automaticamente.

Dados do app Suporte X para preencher no Meta:

```text
Nome do pacote: com.suportex.app
Hash de assinatura debug local: 3GgmSBLDRF3
```

Para producao via Play Store, use o certificado **App signing key certificate** da Play Console, nao o debug nem a chave de upload local. Calcule o hash de 11 caracteres com o mesmo algoritmo do SMS Retriever/WhatsApp OTP e cadastre esse hash no Meta. O Meta permite adicionar mais de um app/hash; mantenha debug apenas para teste local.
