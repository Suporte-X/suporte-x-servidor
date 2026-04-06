# Regras de Negocio - Cliente / Credito / Atendimento

## 1. Identificacao
- O app Android entra com autenticacao anonima para gerar `clientUid`.
- O cliente pode existir como `uid_<firebase_uid>` e migrar para vinculo por telefone quando houver confirmacao.
- O vinculo app-cliente usa `client_app_links` (uid e, quando houver, ancora de dispositivo).
- Email principal segue opcional e manual.

## 2. Acesso ao suporte
1. Cliente novo: pode entrar na fila (primeiro atendimento gratis).
2. Cliente com `freeFirstSupportUsed == false`: primeiro atendimento liberado.
3. Cliente com `credits > 0`: atendimento liberado.
4. Cliente com `credits == 0` e `freeFirstSupportUsed == true`: bloquear solicitacao e abrir fluxo de compra.

## 3. Fechamento de atendimento
- Ao concluir sessao:
  - se `isFreeFirstSupport == true`: marcar `freeFirstSupportUsed = true`.
  - senao: consumir `creditsConsumed`.
- Atualizar agregados em `client_profiles`:
  - `totalSessions`
  - `totalPaidSessions`
  - `totalFreeSessions`
  - `totalCreditsUsed`
  - `lastSupportAt`

## 4. Verificacao de telefone (PNV)
- Falha de verificacao automatica nao pode bloquear atendimento.
- Fluxo registra rastros em `pnv_requests`.
- Estado atual fica em `client_verifications`.
- Fallback manual deve ser acionado pelo tecnico quando necessario.

## 5. Creditos manuais (painel)
- Supervisor pode adicionar/remover creditos.
- Sempre recalcular `clients.status` conforme `freeFirstSupportUsed` e `credits`.

## 6. Pedidos de compra
- Toda intencao de compra gera item em `credit_orders`.
- `paymentMethod = whatsapp` funcional.
- `paymentMethod = pix|card` em placeholder para integracao futura.

## 7. Nao regressao obrigatoria
- Nao reabrir fallback legado por `socket.id` quando `clientUid` estiver vinculado.
- Rotas de supervisor exigem claim + tecnico ativo.
- Em chamada Android, manter janela de tolerancia para `DISCONNECTED` antes de `FAILED`.

## 8. Retencao de dados operacionais
- `pnv_requests`: 15 dias.
- `support_sessions`: 30 dias (operacional recente).
- `support_reports`: 30 dias.
- Cadastros centrais nao entram em limpeza automatica.
