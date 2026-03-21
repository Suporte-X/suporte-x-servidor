# Regras de Negocio - Cliente/Credito/Atendimento

## 1. Identificacao
- O cliente e identificado por telefone (`clients.phone`).
- Nao ha senha nem login obrigatorio na entrada do app.
- O e-mail principal e opcional e preenchido manualmente pelo tecnico.

## 2. Acesso ao suporte
1. Cliente novo (nao existe em `clients`): pode entrar e usa primeiro atendimento gratis.
2. Cliente existente com `credits > 0`: pode entrar normalmente.
3. Cliente existente com `credits == 0` e `freeFirstSupportUsed == true`: bloqueia entrada e abre compra de creditos.

## 3. Fechamento de atendimento
- Ao concluir sessao:
  - se `isFreeFirstSupport == true`: marcar `freeFirstSupportUsed = true`.
  - senao: consumir `creditsConsumed` do cliente.
- Atualizar `client_profiles`:
  - `totalSessions`
  - `totalPaidSessions` / `totalFreeSessions`
  - `totalCreditsUsed`
  - `lastSupportAt`

## 4. Creditos manuais (painel)
- Supervisor pode:
  - adicionar creditos
  - remover creditos
- Sempre recalcular `clients.status` com base em:
  - `freeFirstSupportUsed`
  - `credits`

## 5. Pedidos de compra
- Toda intencao de compra cria um registro em `credit_orders`.
- Nesta fase:
  - `paymentMethod = whatsapp` ja funcional
  - `paymentMethod = pix|card` com placeholder pronto para integracao futura

## 6. Nao regressao
- Nao alterar regras de sessao que ja estavam estabilizadas:
  - fallback legado por `socket.id` nao deve ser reaberto quando `clientUid` vinculado
  - regras de supervisor e claim seguem obrigatorias
  - janela de tolerancia de `DISCONNECTED` no Android permanece
