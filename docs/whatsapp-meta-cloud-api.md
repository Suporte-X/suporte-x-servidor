# Integração WhatsApp Meta Cloud API

Este roteiro conecta o número WhatsApp Cloud API da Meta ao painel da SupportX para receber mensagens dos clientes e responder pelo botão WhatsApp da Central.

## O que já existe no backend

- Envio via Meta Cloud API usando `WHATSAPP_ACCESS_TOKEN`, `WHATSAPP_PHONE_NUMBER_ID` e `WHATSAPP_API_VERSION`.
- Lista de conversas em `GET /api/whatsapp-api/conversations`.
- Lista de mensagens em `GET /api/whatsapp-api/conversations/:id/messages`.
- Envio manual pelo painel em `POST /api/whatsapp-api/conversations/:id/messages`.
- Recebimento via webhook em `GET/POST /api/whatsapp-api/webhook`.

## Variáveis necessárias no Render

As variáveis já existentes continuam iguais:

- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_API_VERSION`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_TEMPLATE_LANGUAGE`
- `WHATSAPP_TEMPLATE_NAME`
- `WHATSAPP_TEMPLATE_TECH`

Adicionar estas duas:

- `WHATSAPP_WEBHOOK_VERIFY_TOKEN`: frase secreta criada por nós para validar a URL no painel da Meta.
- `META_APP_SECRET`: chave secreta do app Meta, usada para validar a assinatura `x-hub-signature-256` recebida no webhook.

Também é aceito `WHATSAPP_APP_SECRET` como alternativa a `META_APP_SECRET`, mas a recomendação operacional é usar `META_APP_SECRET`.

## URL de callback

Use o domínio público do backend em produção:

```text
https://suportex.app/api/whatsapp-api/webhook
```

Se o backend estiver em outro domínio do Render, use:

```text
https://SEU-DOMINIO-DO-RENDER/api/whatsapp-api/webhook
```

## Passo a passo no Render

1. Abra o serviço backend da SupportX no Render.
2. Entre em `Environment`.
3. Confirme se as variáveis WhatsApp já existem.
4. Clique em `Add Environment Variable`.
5. Crie `WHATSAPP_WEBHOOK_VERIFY_TOKEN` com uma frase longa e difícil de adivinhar.
6. Crie `META_APP_SECRET` com a chave secreta do app Meta.
7. Salve as variáveis.
8. Faça deploy/redeploy do serviço para o backend carregar as novas variáveis.

## Onde pegar o App Secret na Meta

1. Abra `developers.facebook.com`.
2. Entre em `Meus apps`.
3. Selecione o app `Suporte X`.
4. No menu lateral, entre em `Configurações do app`.
5. Clique em `Básico`.
6. Localize `Chave secreta do aplicativo`.
7. Clique em `Mostrar`.
8. Copie o valor e coloque no Render como `META_APP_SECRET`.

Não use o `Token de cliente` da tela `Avançado`; ele não substitui a chave secreta do app.

## Configurar webhook na Meta

1. No app `Suporte X`, entre em `Casos de uso`.
2. Selecione `Conectar no WhatsApp`.
3. Clique em `Configuração`.
4. Na seção `Webhook`, preencha `URL de callback` com a URL pública do webhook.
5. Em `Verificar token`, cole exatamente o valor de `WHATSAPP_WEBHOOK_VERIFY_TOKEN` configurado no Render.
6. Clique em `Verificar e salvar`.
7. Depois da verificação, procure a assinatura/campos do webhook do WhatsApp.
8. Assine o campo `messages`.

## Publicar o app

Enquanto o app estiver `Não publicado`, a Meta não entrega mensagens reais de produção para o webhook. Depois de validar o callback:

1. Entre em `Publicar`.
2. Complete as ações pendentes exigidas pela Meta.
3. Publique o app.
4. Confirme se o número em `Gerenciador do WhatsApp > Telefones` está `Conectado`.

## Validação

1. Envie uma mensagem de um WhatsApp comum para o número Cloud API.
2. Abra a Central da SupportX.
3. Clique no botão `WhatsApp`.
4. A conversa deve aparecer na lista.
5. Abra a conversa e responda pelo painel.
6. Confirme se a resposta chega no WhatsApp do cliente.

## Observações importantes

- Mensagem livre só pode ser enviada dentro da janela de 24 horas após o cliente iniciar ou responder a conversa.
- Fora da janela de 24 horas, use template aprovado na Meta.
- O WhatsApp normal/Business App não gerencia esse número quando ele está conectado como Cloud API, salvo cenários específicos de coexistência habilitados pela Meta.
- Nunca exponha `WHATSAPP_ACCESS_TOKEN`, `META_APP_SECRET` ou `WHATSAPP_WEBHOOK_VERIFY_TOKEN` em print público, GitHub ou frontend.
