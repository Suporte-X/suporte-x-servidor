# Plano de Organizacao Firestore (Sem Quebra)

## Objetivo
Melhorar legibilidade/manutencao sem regredir app Android, painel tecnico e regras de negocio atuais.

## Fase 1 (ja aplicada)
- Manter estrutura atual de colecoes.
- Aplicar retencao em dados temporarios via campo `expiresAt`:
  - `pnv_requests`: 15 dias.
  - `support_sessions`: 30 dias.
  - `support_reports`: 30 dias.
- Preservar dados permanentes:
  - `clients`
  - `client_profiles`
  - `client_verifications`
  - `client_app_links`
  - `techs`

## Fase 2 (recomendada)
- Ativar TTL no Firestore para as colecoes operacionais usando o campo `expiresAt`.
- Rodar limpeza inicial dos documentos antigos:
  - remover `pnv_requests` com `createdAt` acima de 15 dias.
  - remover `support_sessions` finalizadas (`completed|cancelled`) com `createdAt` acima de 30 dias.
  - remover `support_reports` com `createdAt` acima de 30 dias.

## Fase 3 (organizacao estrutural, opcional)
- Consolidar leitura humana de cadastro em `clients`.
- Tratar `client_profiles` como agregados de apoio (ou fundir em `clients` numa migracao controlada).
- Tratar `client_verifications` como estado atual e `pnv_requests` como trilha temporal.
- Manter compatibilidade por periodo de transicao (dual-read/dual-write) antes de remover colecoes antigas.

## Nao regressao obrigatoria
- Nao alterar fluxo de sessao estabilizado.
- Nao alterar regras de supervisor/claim.
- Nao remover dados de cadastro (cliente/tecnico) durante limpeza operacional.
