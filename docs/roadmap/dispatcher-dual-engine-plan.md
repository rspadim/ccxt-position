# Dispatcher Dual-Engine Plan (ccxt + ccxtpro)

## Objetivo
Evoluir o dispatcher para operar dois pools independentes no mesmo processo:
- pool `ccxt` (driver atual, async)
- pool `ccxtpro` (driver pro)

A API pública permanece unificada. O roteamento para engine/pool é decidido por `account_id` -> `account.exchange_id`.

## Escopo da Entrega
- Roteamento e execução por engine com contratos de API inalterados.
- Fan-out multi-conta com agregação única de resposta.
- Observabilidade por engine/pool.
- Testes unitários cobrindo os dois engines.
- Testes de integração via API cobrindo os dois engines.

## Decisões Fechadas
- Sem legado: remover `dispatcher_pool_size` e usar apenas:
  - `dispatcher_pool_size_ccxt`
  - `dispatcher_pool_size_ccxtpro`
- Sem normalização implícita de engine:
  - `exchange_id` sem prefixo (`ccxt.` ou `ccxtpro.`) deve gerar erro explícito.
- Sem fallback automático de `ccxtpro` para `ccxt`.
- `dispatcher_worker_hint` continua 1 coluna, com interpretação local ao engine da conta.
- Lock por conta deve ser por engine/pool (não global entre engines):
  - chave recomendada: `(engine, account_id)`.

## Mudanças de Configuração
Arquivo-alvo: `apps/api/app/config.py`

- Adicionar (obrigatórios):
  - `dispatcher_pool_size_ccxt: int`
  - `dispatcher_pool_size_ccxtpro: int`
- Remover leitura/uso de `dispatcher_pool_size` (legado).
- Validar `>= 1` para ambos.

## Mudanças de Regras de Engine
### Resolução de engine
Fonte oficial: `accounts.exchange_id`.

Regras:
- `ccxt.binance` -> engine `ccxt`
- `ccxtpro.binance` -> engine `ccxtpro`
- qualquer valor sem prefixo válido -> erro `unsupported_engine`

### Disponibilidade do módulo
Se engine for `ccxtpro` e módulo indisponível no processo:
- retornar erro `engine_unavailable`.

## Arquitetura Interna do Dispatcher
Arquivo principal: `apps/api/dispatcher_server.py`

### Estado por engine
Substituir estruturas únicas por estruturas indexadas por engine:
- `worker_queues[engine][worker_id]`
- `worker_tasks[engine][worker_id]`
- `worker_inflight[engine][worker_id]`
- `worker_active_accounts[engine][worker_id]`
- `account_worker[(engine, account_id)] = worker_id`
- locks: `account_locks[(engine, account_id)]`

### Seleção de worker
`_resolve_worker_for_account(account_id)` deve retornar `(engine, worker_id)`:
1. resolve engine da conta
2. tenta cache `(engine, account_id)`
3. tenta `dispatcher_worker_hint` persistido
4. valida range pelo pool do engine
5. fallback `least_loaded` dentro do engine
6. persiste hint

### Execução por engine
No ponto de execução de comando exchange:
- branch explícito por engine:
  - `ccxt` -> caminho atual
  - `ccxtpro` -> caminho pro
- mesmo contrato de entrada/saída para API

## Multi-Conta e Batch
Para operações com `account_ids` múltiplos:
- expandir por conta
- despachar cada conta no engine correto
- executar concorrente entre contas/pools
- agregar mantendo ordem por `index` do request

## Banco e Persistência
- Manter `dispatcher_worker_hint` como inteiro único.
- Engine não precisa de nova coluna obrigatória; vem de `exchange_id`.
- Se no futuro quiser observabilidade SQL direta, pode adicionar coluna derivada/materializada de engine (fora deste escopo).

## Observabilidade
- Métricas por engine:
  - tamanho do pool
  - inflight por worker
  - queue depth por worker
  - accounts mapeadas
- Logs por engine + hint:
  - `dispatcher-ccxt-hint-{id}.log`
  - `dispatcher-ccxtpro-hint-{id}.log`
- Status do sistema deve expor saúde separada por engine.

## Erros (contrato)
Adicionar/padronizar:
- `unsupported_engine`
- `engine_unavailable`
- `account_engine_mismatch` (se aplicável)

## Plano de Implementação
1. Config
- adicionar novos campos e remover legado
- ajustar docs/config examples

2. Engine resolver
- validar prefixo estrito
- remover normalização implícita

3. Pool duplo
- refatorar estruturas internas para `dict[engine]`
- iniciar loops/workers por engine

4. Hint + lock
- ajustar cache/persistência de hint por contexto de engine
- migrar lock para `(engine, account_id)`

5. Execução
- separar caminho de execução por engine
- manter payload/resposta idênticos para API

6. Batch multi-engine
- revisar operações multi-conta para fan-out cross-engine
- garantir agregação estável

7. Observabilidade
- logs por engine/hint
- status por engine

8. Testes
- unitários + integração API

## Plano por Arquivo (Decision Complete)
### `apps/api/app/config.py`
- Remover `dispatcher_pool_size`.
- Adicionar:
  - `dispatcher_pool_size_ccxt`
  - `dispatcher_pool_size_ccxtpro`
- Ajustar loader (`env`/`yaml`) para exigir ambos.
- Validar limites (`>= 1`).

### `docs/ops/configuration-reference.md`
- Remover documentação de `dispatcher_pool_size`.
- Documentar os dois novos campos com exemplos.
- Deixar explícito que não há fallback legado.

### `apps/api/dispatcher_server.py`
- Introduzir tipo interno de engine (`ccxt`, `ccxtpro`).
- Trocar estruturas globais por estruturas por engine.
- Refatorar `start()` para criar workers por engine e por tamanho de pool.
- Refatorar `stop()` para encerrar workers de ambos os engines.
- Refatorar resolução de worker para retornar `(engine, worker_id)`.
- Ajustar persistência/leitura de hint sem mudar schema:
  - hint permanece inteiro.
  - validado no range do engine atual da conta.
- Trocar lock para chave `(engine, account_id)`.
- Separar caminho de execução por engine mantendo o mesmo contrato de payload/resposta.
- Ajustar nomes de logger por engine.
- Expor status por engine no payload de health/status.

### `apps/api/app/ccxt_adapter.py` (ou adapter equivalente pro)
- Garantir interface comum para os dois caminhos de execução.
- Adicionar ramificação explícita para `ccxtpro` nas operações necessárias.
- Garantir que erros de indisponibilidade virem `engine_unavailable`.

### `apps/api/app/repository_mysql.py`
- Sem mudança obrigatória de schema.
- Garantir leitura de `exchange_id` sem normalização implícita.
- Opcional fora do escopo: coluna derivada de engine para observabilidade SQL.

### `docs/api/error-model.md`
- Adicionar:
  - `unsupported_engine`
  - `engine_unavailable`
  - `account_engine_mismatch` (quando aplicável)

### `test/` (unit e integração)
- Incluir cobertura de dual-engine em seleção de worker, lock e batch.
- Incluir cobertura API com contas de engines diferentes na mesma request.
- Incluir casos de erro de engine inválido e engine indisponível.

## Checklist de Execução
- [x] Config dual-pool implementada sem fallback legado.
- [x] Resolução de engine estrita (sem normalização silenciosa).
- [x] Estado interno do dispatcher separado por engine.
- [x] Locking por `(engine, account_id)` implementado.
- [x] Roteamento e execução por engine funcionando em comandos single-account.
- [x] Fan-out multi-conta cross-engine funcionando com agregação estável por `index`.
- [x] Logs por engine+hint implementados.
- [x] Status do sistema com métricas por engine.
- [x] Erros novos documentados e retornando corretamente.
- [x] Testes unitários dual-engine passando.
- [x] Testes de integração via API dual-engine passando.

## Estratégia de Validação (runbook curto)
### Pré-deploy
1. Subir ambiente com `dispatcher_pool_size_ccxt` e `dispatcher_pool_size_ccxtpro`.
2. Verificar que ausência de qualquer um dos dois quebra startup (esperado).
3. Rodar suíte unitária dual-engine.
4. Rodar suíte de integração API dual-engine.

### Pós-deploy
1. Validar `/admin/system/status` (ou equivalente) com métricas dos dois engines.
2. Enviar comando para conta `ccxt.*` e confirmar execução/log no pool `ccxt`.
3. Enviar comando para conta `ccxtpro.*` e confirmar execução/log no pool `ccxtpro`.
4. Enviar batch com contas mistas e confirmar agregação correta por `index`.
5. Validar erro `unsupported_engine` em conta com prefixo inválido.
6. Validar `engine_unavailable` desligando disponibilidade do módulo pro (ambiente de teste).

## Fora de Escopo (nesta entrega)
- Lock distribuído entre múltiplos processos/hosts.
- Migração de schema para separar hint por engine.
- Auto-healing de `exchange_id` inválido.
- Fallback automático de `ccxtpro` para `ccxt`.

## Testes Unitários (obrigatórios)
1. Resolução de engine
- aceita `ccxt.*` e `ccxtpro.*`
- rejeita sem prefixo

2. Seleção de worker
- respeita range de pool por engine
- hint inválido faz fallback least-loaded no engine correto

3. Locking
- mesma conta + mesmo engine serializa
- mesma conta + engines diferentes não bloqueiam entre si

4. Erros
- `ccxtpro` indisponível retorna `engine_unavailable`

5. Batch
- request multi-conta mistura engines e agrega corretamente por `index`

## Testes de Integração via API (obrigatórios)
1. Comando single-account em conta `ccxt.*` executa no pool `ccxt`.
2. Comando single-account em conta `ccxtpro.*` executa no pool `ccxtpro`.
3. Batch multi-conta com engines mistos retorna com ordenação estável e sem perda.
4. Conta com `exchange_id` sem prefixo retorna `unsupported_engine`.
5. Conta `ccxtpro.*` sem módulo disponível retorna `engine_unavailable`.
6. Endpoint/status expõe telemetria separada por engine.

## Critérios de Aceite
- API não muda para cliente.
- Dois pools funcionam em paralelo no mesmo processo.
- Sem regressão em fluxo `ccxt` existente.
- `ccxtpro` usa caminho dedicado sem fallback implícito.
- Todos os testes unitários e de integração citados acima passam.

## Riscos e Mitigações
- Risco: divergência de comportamento entre drivers.
  - Mitigação: adapter interface comum + testes de contrato por engine.

- Risco: batch multi-conta mascarar erro parcial.
  - Mitigação: manter retorno por item com `index` e erro detalhado.

- Risco: lock errado degradar throughput.
  - Mitigação: lock por `(engine, account_id)` validado por testes concorrentes.

## Nota sobre lock e processo
Neste projeto, o lock discutido é dentro do processo do dispatcher (tasks/workers async). Não é lock distribuído entre processos distintos. Com lock por `(engine, account_id)`, o pool `ccxt` não bloqueia o `ccxtpro` para a mesma conta lógica.
