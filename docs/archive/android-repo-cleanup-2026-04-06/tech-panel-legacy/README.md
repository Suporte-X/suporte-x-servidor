# Painel Tecnico Web (MVP)

Painel simples para:
- visualizar novas solicitacoes (`support_sessions`);
- cadastrar cliente rapidamente (`clients`);
- consultar ficha completa;
- acompanhar status de verificacao (`client_verifications` e `pnv_requests`);
- ajustar creditos manualmente;
- acionar fallback manual de verificacao quando necessario;
- registrar encerramento de atendimento;
- visualizar pedidos de credito (`credit_orders`).

## Como usar
1. Abra [`index.html`](./index.html) em um servidor estatico.
2. Defina `window.SUPORTEX_FIREBASE_CONFIG` com as credenciais do projeto.
3. Garanta permissao Firestore para o usuario autenticado (ideal: claim supervisor).

## Observacoes
- O painel usa os mesmos nomes de colecao do app Android.
- O e-mail principal e opcional e manual.
- Pagamentos PIX/cartao continuam como placeholder (estrutura pronta).
- Compra por WhatsApp ja aparece em `credit_orders` com `whatsappRequested = true`.
