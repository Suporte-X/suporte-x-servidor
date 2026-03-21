# Regras de Negócio - Cliente / Crédito / Atendimento

## 1. Identificação
- O cliente novo abre o app e pode solicitar suporte sem informar telefone/nome na home.
- A identificação principal é conduzida pelo técnico no cadastro inicial do atendimento.
- Após cadastro, o vínculo app-cliente é salvo em `client_app_links` via `clientUid`.
- E-mail continua opcional e manual.

## 2. Acesso ao suporte
1. Cliente não cadastrado: pode entrar na fila (primeiro atendimento liberado).
2. Cliente cadastrado com `freeFirstSupportUsed == false`: pode entrar (primeiro grátis pendente).
3. Cliente cadastrado com `credits > 0`: pode entrar normalmente.
4. Cliente cadastrado com `credits == 0` e `freeFirstSupportUsed == true`: bloqueia solicitação (sem crédito).

## 3. Fechamento de atendimento
- Ao concluir sessão:
  - se `isFreeFirstSupport == true`: marcar `freeFirstSupportUsed = true`.
  - senão: consumir `creditsConsumed` do cliente.
- Atualizar `client_profiles`:
  - `totalSessions`
  - `totalPaidSessions` / `totalFreeSessions`
  - `totalCreditsUsed`
  - `lastSupportAt`

## 4. Verificação pós-cadastro (PNV)
- Ao técnico cadastrar o cliente, o sistema:
  - atualiza `client_verifications` com `status = pending`;
  - cria pedido em `pnv_requests` para processamento no app.
- Resultado visual no painel:
  - verde: `verified`
  - vermelho: `pending | mismatch | manual_required`
- Se vermelho, o técnico pode iniciar fallback manual.

## 5. Créditos manuais (painel)
- Supervisor pode:
  - adicionar créditos
  - remover créditos
- Sempre recalcular `clients.status` com base em:
  - `freeFirstSupportUsed`
  - `credits`

## 6. Pedidos de compra
- Toda intenção de compra cria um registro em `credit_orders`.
- Nesta fase:
  - `paymentMethod = whatsapp` já funcional
  - `paymentMethod = pix|card` com placeholder pronto para integração futura

## 7. Não regressão
- Não alterar regras de sessão já estabilizadas:
  - fallback legado por `socket.id` não deve ser reaberto quando `clientUid` vinculado
  - regras de supervisor e claim seguem obrigatórias
  - janela de tolerância de `DISCONNECTED` no Android permanece
