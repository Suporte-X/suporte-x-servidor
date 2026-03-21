# Suporte X - Modelagem Firestore

Esta pasta documenta a estrutura de dados usada no fluxo atual de cliente, créditos, cadastro técnico e verificação.

## collections/clients
- `id` (document id): `phone_<somente_digitos>`
- `phone`: telefone principal do cliente
- `name`: nome do cliente
- `primaryEmail` (opcional/manual)
- `notes`: observações administrativas
- `credits`: créditos disponíveis
- `supportsUsed`: atendimentos utilizados
- `freeFirstSupportUsed`: primeiro atendimento grátis já usado
- `status`: `first_support_pending | with_credit | without_credit`
- `createdAt`
- `updatedAt`

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

## collections/client_app_links
- `id` (document id): `clientUid` do app
- `clientUid`
- `clientId`
- `phone`
- `supportSessionId` (última sessão vinculada)
- `createdAt`
- `updatedAt`

## collections/client_verifications
- `id` (document id): `clientId`
- `clientId`
- `primaryPhone` (telefone informado no cadastro técnico)
- `verifiedPhone` (telefone validado no fluxo de verificação)
- `status`: `pending | verified | mismatch | manual_required`
- `source`
- `mismatchReason` (quando divergente)
- `lastTriggerAt`
- `lastVerificationAt`
- `updatedAt`

## collections/pnv_requests
- `id`
- `clientId`
- `clientUid`
- `supportSessionId`
- `manualFallback`
- `status`: `pending | manual_pending | processed`
- `createdAt`
- `processedAt`
- `updatedAt`

## collections/support_sessions
- `id`
- `clientId` (pode iniciar vazio para cliente novo)
- `clientPhone`
- `clientName`
- `clientUid`
- `sessionId` (session realtime/socket, quando existir)
- `techId`
- `techName`
- `startedAt`
- `endedAt`
- `status`: `queued | in_progress | completed | cancelled`
- `requiresTechnicianRegistration`
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

## collections/support_reports
- `id`
- `sessionId`
- `clientId`
- `techId`
- `createdAt`
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
