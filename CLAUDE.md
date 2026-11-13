# Semaphore

Semaphore runs environmental prediction models (neural nets) on a schedule: it ingests
data, builds input vectors, executes the model, and stores the outputs.
Each model's requirements are described by a JSON **dspec**.

**Domain model:** read `docs/domain-model.md` before reasoning about models, runs, inputs,
or outputs. It is canonical (the team wiki links to it) and describes intent; the code
does not fully match it yet. It is the shared language between domain experts and
engineers: use its terms when naming new code, database fields, and config.

## Glossary Notes
- **`p*` / `d*` series** (e.g. `pAirTemp`, `dWaterTmp`): `p` = predicted, `d` = observed.
- **`outKey`**: the identifier that cross-references a series inside a dspec
  (`dependentSeries`, `postProcessCall.args.targetSeries`, `vectorOrder`). Hyphen/underscore,
  no spaces. `_name` is only a human label and uses spaces. Don't make the two match.
  Despite the name, an `outKey` doesn't make a series an Out-Series in the domain-model
  sense: only the series listed in `vectorOrder` are. The dspec format predates the domain
  model and doesn't follow it closely.

## Map
- `src/semaphore/`: `semaphoreRunner.py` (CLI entry), `orchestrator.py`, and packages
  `DataIngestion`, `DataIntegrity`, `DataValidation`, `ModelExecution`, `PostProcessing`,
  `SeriesProvider`, `SeriesStorage`.
- `api/apiDriver.py`: the API service. `tools/`: deploy, cron, and migration scripts.
- `data/dspec/`: dspecs, mostly grouped into family folders that change over time. Some
  families have subfolders and some files sit at the top level. A question about "the dspecs"
  means all of them: search `data/dspec/` recursively first.

## Running things (always from the repo root; paths are relative)
- Tests, host: `python -m pytest` (venv with `pip install -e '.[test]'`, DB container up).
  Container: `docker exec semaphore-core python3 -m pytest`.
  `pytest` = unit tests only; `pytest tests --run-slow` adds slow tests and integration tests.
  Integration tests call live external services, so a failure may be an outage.
- Run a model without saving: `python -m semaphore.semaphoreRunner -d <dspec path> -t -v`.
- The container only mounts `data/`. Code changes reach it only via `docker compose watch`
  (plain `up` ignores the watch config) or a rebuild. Check for stale code before trusting
  a container result.

## Working conventions
- Don't design logic around what today's dspecs happen to contain. Design for the stated
  semantics, since a future dspec may fall outside the current range.
- Execute multi-step plans one step at a time and stop after each. Never `git commit`;
  the user reviews and commits.
- Verified docs: `docs/domain-model.md` and `docs/semaphore_db_schema.sql` (DB
  schema snapshot). The schema's header names the last migration it includes. Check
  `tools/DatabaseMigration/` for any later migrations before trusting it as current.
  Any other doc (in `docs/`, `README.md`, etc.) is unverified until it is added here. Check
  its claims against the code before relying on them.
