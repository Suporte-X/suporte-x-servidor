# Suporte X - Modelagem Firestore

Esta pasta documenta a estrutura de dados usada no cadastro leve de clientes, creditos e cobranca futura.

## collections/clients
- `id` (document id): `phone_<somente_digitos>`
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
