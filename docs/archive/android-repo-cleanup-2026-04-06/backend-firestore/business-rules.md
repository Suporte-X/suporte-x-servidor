# Regras de Negocio - Cliente/Credito/Atendimento

## 1. Identificacao
- O app cria sessao anonima automaticamente (sem tela de login) para gerar `clientUid`.
- O cliente pode iniciar como conta provisoria por UID (`clients.id = uid_<firebase_uid>`).
- Quando houver telefone verificado por Firebase PNV, o vinculo passa a priorizar `clients.phone`.
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

## 7. Verificacao de telefone (PNV)
- O atendimento nao deve ser bloqueado por falha de verificacao automatica.
- O app tenta PNV apos solicitar suporte e registra rastros em `pnv_requests`.
- Status de verificacao do cliente fica em `client_verifications`.
- Fallback manual deve ser acionado somente pelo tecnico quando necessario.

## 8. Retencao de dados operacionais
- `pnv_requests`: manter por 15 dias.
- `support_sessions`: manter por 30 dias para trilha operacional recente.
- `support_reports`: manter por 30 dias.
- Cadastros centrais (`clients`, `client_profiles`, `client_verifications`, `client_app_links`) nao entram em limpeza automatica.
