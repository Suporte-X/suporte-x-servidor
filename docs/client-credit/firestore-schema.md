# Suporte X - Modelagem Firestore

Este documento descreve a estrutura ativa usada por Android + Central Web.

## collections/clients
- `id` (document id): `phone_<somente_digitos>` ou `uid_<firebase_uid>`
- `phone`: telefone principal do cliente
- `name`
- `primaryEmail` (opcional/manual)
- `notes`
- `credits`
- `supportsUsed`
- `freeFirstSupportUsed`
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
- `id` (document id): `clientUid` ou `device_<anchor>`
- `clientUid`
- `clientId`
- `phone`
- `deviceAnchor`
- `supportSessionId` (opcional)
- `linkType` (opcional, usado em doc de dispositivo)
- `createdAt`
- `updatedAt`

## collections/client_verifications
- `id` (document id): `clientId`
- `clientId`
- `primaryPhone`
- `verifiedPhone`
- `status`: `pending | verified | mismatch | manual_required`
- `source` (opcional)
- `mismatchReason` (opcional)
- `lastTriggerAt` (opcional)
- `lastVerificationAt`
- `updatedAt`

## collections/pnv_requests
- `id`
- `clientUid` (opcional)
- `clientId` (opcional)
- `phone` (opcional)
- `status`: `pending | manual_pending | processed`
- `manualFallback`
- `reason` (opcional)
- `source` (opcional)
- `tokenPresent` (opcional)
- `createdAt`
- `processedAt` (opcional)
- `updatedAt`
- `expiresAt` (opcional; recomendado para TTL)

## collections/support_sessions
- `id`
- `clientId` (pode iniciar vazio para cliente novo)
- `clientPhone`
- `clientName`
- `clientUid`
- `sessionId` (session realtime/socket)
- `techId`
- `techName`
- `startedAt`
- `acceptedAt` (opcional; quando o tecnico aceita)
- `endedAt`
- `status`: `queued | in_progress | completed | cancelled`
- `requiresTechnicianRegistration`
- `isFreeFirstSupport`
- `creditsConsumed`
- `problemSummary`
- `solutionSummary`
- `internalNotes`
- `reportId`
- `device` (brand/model/androidVersion/anchor)
- `createdAt`
- `updatedAt`
- `billingAppliedAt` (opcional)
- `expiresAt` (opcional; recomendado para TTL de finalizadas)

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
- `expiresAt` (opcional; recomendado para TTL)

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

## Retencao recomendada (TTL)
- `pnv_requests`: 15 dias
- `support_sessions`: 30 dias (somente finalizadas/canceladas)
- `support_reports`: 30 dias
- Cadastros principais (`clients`, `client_profiles`, `client_verifications`, `client_app_links`, `credit_orders`, `credit_packages`) sem TTL automatico.

## Limpeza operacional

Script disponivel em:
- `server/scripts/firestoreRetentionCleanup.mjs`

Uso:
- `node server/scripts/firestoreRetentionCleanup.mjs`
- `node server/scripts/firestoreRetentionCleanup.mjs --execute`
