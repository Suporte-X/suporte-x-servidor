# Relatorio ao Cliente no Encerramento

Este documento descreve como habilitar o disparo automatico do relatorio de atendimento para o cliente via WhatsApp e e-mail.

## O que foi implementado

- Ao encerrar a sessao em `POST /api/sessions/:id/close`, o backend tenta enviar o relatorio para:
  - WhatsApp (Cloud API);
  - e-mail (Resend).
- A nota de satisfacao do tecnico **nao** entra no relatorio enviado ao cliente.
- O resultado de envio (sucesso/erro por canal) fica salvo em `sessions/{id}.clientReport`.
- A Central agora tem o botao `Baixar relatorio (PDF)` em `Meus relatorios > Detalhe da sessao`.
- A Central agora tem os botoes:
  - `Enviar/Reenviar e-mail`;
  - `Enviar/Reenviar WhatsApp`.
  Esses botoes usam `POST /api/reports/sessions/:id/send-client-report` com `channel=email|whatsapp|both`.

## Variaveis de ambiente

### WhatsApp (Meta Cloud API)

- `WHATSAPP_ACCESS_TOKEN`: token da conta WhatsApp Business.
- `WHATSAPP_PHONE_NUMBER_ID`: phone number id do numero emissor.
- `WHATSAPP_API_VERSION` (opcional): padrao `v21.0`.
- `WHATSAPP_TEMPLATE_NAME` (opcional): nome do template aprovado no Meta.
  - Padrao no backend: `relatorio_de_atendimento`.
- `WHATSAPP_TEMPLATE_LANGUAGE` (opcional): idioma do template.
  - Padrao no backend: `pt_BR`.
- `SUPPORT_REPORT_WHATSAPP_FORCE_TO` (opcional): numero fixo para testes (ex.: `+5565999999999`).

### E-mail (Resend)

- `RESEND_API_KEY`: chave da API Resend.
- `SUPPORT_REPORT_EMAIL_FROM`: remetente exibido no e-mail.
  - Exemplo com dominio Xavier: `Suporte X <no-reply@xavierassessoriadigital.com.br>`.
- `SUPPORT_REPORT_EMAIL_REPLY_TO` (opcional): e-mail de resposta.

### Geral

- `SUPPORT_REPORT_TIMEZONE` (opcional): padrao `America/Cuiaba`.

## Requisitos de dados do cliente

- WhatsApp: `clientPhone` valido na sessao/cliente.
- E-mail: `primaryEmail` no cadastro do cliente.
- O envio automatico ocorre quando houver resumo de fechamento (sintoma e/ou solucao).

## Validacao rapida

1. Abra uma sessao real no painel tecnico.
2. Preencha `Sintoma principal` e `Solucao aplicada`.
3. Clique em `Encerrar suporte, disparar pesquisa e enviar relatorio`.
4. Verifique:
   - toast na Central com o resultado do envio;
   - documento da sessao no Firestore em `clientReport`;
   - recebimento no WhatsApp/e-mail do cliente.

## Download PDF no painel

1. Abra `Meus relatorios`.
2. Selecione uma sessao encerrada.
3. Clique em `Baixar relatorio (PDF)`.
4. O PDF e gerado por `GET /api/reports/sessions/:id/pdf`.
