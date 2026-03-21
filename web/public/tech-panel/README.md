# Painel Técnico Web (MVP)

Painel para:
- visualizar novas solicitações (`support_sessions`);
- aceitar atendimento sem sair da tela;
- abrir cadastro inicial do cliente dentro do próprio painel;
- disparar verificação pós-cadastro em segundo plano (`client_verifications` + `pnv_requests`);
- usar fallback manual de verificação quando necessário;
- consultar ficha completa, histórico e pedidos de crédito.

## Como usar
1. Abra [`index.html`](./index.html) em um servidor estático.
2. Defina `window.SUPORTEX_FIREBASE_CONFIG` com as credenciais do projeto.
3. Garanta permissão Firestore para o usuário autenticado (ideal: claim supervisor).

## Observações
- O painel usa os mesmos nomes de coleção do app Android.
- O cadastro inicial é guiado pelo técnico após aceitar o atendimento.
- O indicador de verificação usa ponto verde/vermelho no card/ficha do cliente.
- Pagamentos PIX/cartão continuam como placeholder.
