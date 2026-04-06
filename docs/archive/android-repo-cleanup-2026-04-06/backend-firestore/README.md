# Suporte X - Modelagem Firestore

Esta pasta documenta a estrutura de dados usada no cadastro leve de clientes, creditos e cobranca futura.

## collections/clients
- `id` (document id): `phone_<somente_digitos>` ou `uid_<firebase_uid>`
- `phone`: telefone principal do cliente (base de identificacao)
- `name`: nome do cliente
- `primaryEmail` (opcional/manual)
- `notes`: observacoes administrativas
- `credits`: creditos disponiveis
- `supportsUsed`: atendimentos utilizados
- `freeFirstSupportUsed`: primeiro atendimento gratis ja usado
- `status`: `first_support_pending | with_credit | without_credit`
- `createdAt`
- `updatedAt`

## collections/client_app_links
- `id` (document id): `clientUid`
- `clientUid`: UID anonimo do app Android
- `clientId`: referencia logica ao cliente (`clients/<id>`)
- `phone`: telefone conhecido no vinculo (quando existir)
- `createdAt`
- `updatedAt`

## collections/client_verifications
- `id` (document id): `clientId`
- `clientId`
- `primaryPhone`
- `verifiedPhone`
- `status`: `pending | verified | mismatch | manual_required`
- `mismatchReason` (opcional)
- `lastVerificationAt`
- `updatedAt`

## collections/pnv_requests
- `id`
- `clientUid` (opcional)
- `clientId` (opcional)
- `phone` (opcional)
- `status`: `pending | manual_pending | processed`
- `manualFallback`: `true | false`
- `reason` (opcional)
- `source`: `android_app | android_pnv_sdk | tech_panel`
- `createdAt`
- `updatedAt`
- `expiresAt` (Timestamp; sugerido para TTL de 15 dias)

## collections/client_profiles
- `clientId`
- `totalSessions`
- `totalPaidSessions`
- `totalFreeSessions`
- `totalCreditsPurchased`
- `totalCreditsUsed`
- `lastSupportAt`
- `createdAt`
- `updatedAt`

## collections/support_sessions
- `id`
- `clientId`
- `clientPhone`
- `clientName`
- `clientUid`
- `sessionId` (session realtime/socket, quando existir)
- `techId`
- `techName`
- `startedAt`
- `acceptedAt` (momento em que o tecnico aceita e o atendimento entra em progresso)
- `endedAt`
- `status`: `queued | in_progress | completed | cancelled`
- `isFreeFirstSupport`
- `creditsConsumed`
- `problemSummary`
- `solutionSummary`
- `internalNotes`
- `reportId`
- `device` (brand/model/androidVersion)
- `createdAt`
- `updatedAt`
- `billingAppliedAt`
- `expiresAt` (Timestamp; sugerido para TTL de 30 dias em sessoes finalizadas)

## collections/support_reports
- `id`
- `sessionId`
- `clientId`
- `techId`
- `createdAt`
- `expiresAt` (Timestamp; sugerido para TTL de 30 dias)
- `summary`
- `actionsTaken`
- `solutionApplied`
- `followUpNeeded`

## collections/credit_packages
- `id`
- `name`
- `supportCount`
- `priceCents`
- `active`
- `displayOrder`
- `updatedAt`

## collections/credit_orders
- `id`
- `clientId`
- `packageId`
- `status`: `pending | paid | cancelled`
- `paymentMethod`: `whatsapp | pix | card`
- `amountCents`
- `createdAt`
- `updatedAt`
- `whatsappRequested`
- `pixPlaceholder`
- `cardPlaceholder`

## Politica de retencao recomendada (TTL Firestore)
- `pnv_requests`: 15 dias
- `support_sessions`: 30 dias (somente para dados operacionais; nao usar para cadastro principal)
- `support_reports`: 30 dias
- `clients`, `client_profiles`, `client_verifications`, `client_app_links`, `credit_orders`, `credit_packages`: sem TTL automatico

## Limpeza inicial (legado ja gravado)
Script local para apagar somente dados operacionais antigos:

1. Instalar dependencia:
   - `npm install firebase-admin`
2. Rodar simulacao (nao apaga):
   - `node tools/firestore-retention-cleanup.mjs`
3. Executar limpeza real:
   - `node tools/firestore-retention-cleanup.mjs --execute`

Opcional:
- `--project-id <id>`
- `--pnv-days 15 --support-session-days 30 --support-report-days 30`
