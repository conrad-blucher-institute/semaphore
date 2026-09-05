# Logging & Exception Handling — Per-Module Inventory

Purpose: a per-file inventory of logging and exception-handling usage across `src/` (excludes
`src/API` and `src/tests`), companion to `logging-working-notes.md` and `logging-information.md`.
For each non-trivial file: whether/how it uses the custom logger from `src/utility.py`
(`log`/`log_error`/`log_success`, or raw `print()` instead), and how it catches, logs, swallows,
re-raises, or converts exceptions (including the custom `Semaphore_Exception` /
`Semaphore_Data_Exception` / `Semaphore_Ingestion_Exception` from `src/exceptions.py`).

## Root-level files

**`orchestrator.py`**
- Logging: heaviest user in the codebase (23 call sites) — `log`, `log_error`, `log_success` all used.
- Exceptions: nested try/except in `run_semaphore` — inner `except Exception:` normalizes any
  unanticipated error into `Semaphore_Exception`; outer block catches all three custom exception
  types by name, each logging a headline + exception message + `traceback.format_exc()` via
  `log_error`, then routes to `__handle_failed_prediction`. `__handle_successful_prediction` and
  `__handle_failed_prediction` also each wrap DB-write logic in a bare `except:` that only calls
  `log_error(...)` — swallowed, no re-raise. `__safe_discord_notification` similarly wraps its logic
  in `except Exception as e:` and only logs.

**`semaphoreRunner.py`** (CLI entry point)
- Logging: does **not** use `log`/`log_error`/`log_success` at all. Uses raw `print()` once (verbose
  mode confirmation message).
- Exceptions: one try/except around parsing the `-p/--past` argument — catches (bare `except:`)
  and converts into a differently-worded `ValueError`, which is left to propagate uncaught (crashes
  the CLI with a traceback if hit).

**`discord.py`**
- Logging: one `log()` call — only fires if the Discord webhook doesn't return a 200.
- Exceptions: no try/except here; failure handling is the caller's job (`orchestrator.py`'s
  `__safe_discord_notification` wraps calls into this module).

**`exceptions.py`**
- Logging: none.
- Exceptions: defines the hierarchy itself — `Semaphore_Exception`, `Semaphore_Data_Exception`,
  `Semaphore_Ingestion_Exception`, all subclassing `BaseException` (not `Exception`). Two of the
  three capture `sys.exc_info()`/`traceback.format_exc()` at construction time and embed it in
  `self.message`.

**`utility.py`**
- The logger itself (`log`, `log_error`, `log_success`, `LogLocationDirector`,
  `VerbosityController`) — see the other two docs for behavior detail. One raw `print()` inside
  `log()` (the actual stdout output path, not a bypass).
- Exceptions: none — no try/except in this file.

**`DataClasses.py`** — trivial: no logging, no exception handling (the one docstring mention of
"Exception" is just a parameter type annotation, not real handling).

## DataIngestion

**`DataIngestion/IDataIngestion.py`** (interface + factory)
- Logging: none.
- Exceptions: `data_ingestion_factory` wraps the dynamic module import in try/except; catches
  `ModuleNotFoundError` and re-raises it as a new `ModuleNotFoundError` with a clearer message. Not
  logged anywhere.

**`DataIngestion/DI_Classes/NDFD.py`**
- Logging: 8 `log()` calls, plus 2 raw `print()` calls (unconditionally prints a URL and raw
  response text in a constructor — debug leftovers, unrelated to error handling).
- Exceptions: extensive try/except. Low-level parsing/formatting errors (`AttributeError`,
  `TypeError`, `ValueError`, `IndexError`, `KeyError`, `OSError` combinations) are caught and
  **re-raised as `ValueError`** with a clarifying message (no logging at that point). Separately,
  the HTTP-fetch method catches `HTTPError`/`Exception`, logs via `log()` (message only, no
  traceback), and **swallows** — returns `None` instead of re-raising. The top-level
  `fetch_predictions` method similarly catches `ValueError`/`Exception`, logs, and returns `None`
  (swallowed).

**`DataIngestion/DI_Classes/NDFD_EXP.py`** (near-duplicate of `NDFD.py`)
- Logging: 9 `log()` calls, 1 raw `print()`.
- Exceptions: same re-raise-as-`ValueError` pattern as `NDFD.py`, plus the same swallow-log-return-None
  pattern for fetch failures. One handler additionally does a **raw `print(exc_type, fname,
  tb_lineno)` and `traceback.print_exc()`** directly to stdout instead of going through the logger —
  a second, uncoordinated way exception detail reaches the console. Also raises
  `Semaphore_Ingestion_Exception` for invalid date ranges.

**`DataIngestion/DI_Classes/NDFD_JSON.py`**
- Logging: 3 `log()` calls.
- Exceptions: mixed pattern — some handlers catch and **re-raise** (`except HTTPError: log(...);
  raise`) preserving propagation, while others catch and **convert** into `ValueError` (parsing/lookup
  failures) or raise `Semaphore_Ingestion_Exception` directly (invalid date range) without logging.
  Unlike `NDFD.py`/`NDFD_EXP.py`, failures here aren't silently swallowed — they keep propagating.

**`DataIngestion/DI_Classes/LIGHTHOUSE.py`**
- Logging: 5 `log()` calls.
- Exceptions: one try/except around the fetch call — catches `HTTPError`/`Exception`, logs
  (message only), and swallows (implicit `None` return). Also raises `NotImplementedError` directly
  (not caught locally) when a datum code has no mapping.

**`DataIngestion/DI_Classes/NOAATANDC.py`**
- Logging: 4 `log()` calls (all informational/warning, not exception-related).
- Exceptions: one try/except catching `ValueError`, logged via `log()` only — no re-raise, no
  traceback.

**`DataIngestion/DI_Classes/TWC.py`**
- Logging: none — no use of `log`/`log_error`/`log_success` at all.
- Exceptions: catches `HTTPError`/`URLError`/`json.JSONDecodeError` and converts each into
  `Semaphore_Ingestion_Exception` with no logging at the point of catch; also raises
  `Semaphore_Ingestion_Exception` directly in several guard clauses (missing API key, invalid time
  range, unexpected response shape).

**`DataIngestion/DI_Classes/NDBC.py`**
- Logging: 1 `log()` call (warning on empty data).
- Exceptions: no try/except; raises `NotImplementedError` directly for an unmapped request. Calls
  `response.raise_for_status()`, which can raise but isn't caught in this file.

**`DataIngestion/DI_Classes/SEMAPHORE.py`**
- Logging: imports `log` but **never calls it** — dead import.
- Exceptions: none — no try/except, no raises.

Trivial (no logging, no exception handling): `DataIngestion/__init__.py`.

## DataIntegrity

**`DataIntegrity/IDataIntegrity.py`** (interface + factory)
- Logging: none.
- Exceptions: factory wraps dynamic import in try/except; catches `(ModuleNotFoundError,
  AttributeError)` and converts to `ImportError` with a clearer message. Not logged.

**`DataIntegrity/DataIntegrityClasses/AngleInterpolation.py`**
- Logging: 2 `log()` calls — but **not** inside any try/except; these are plain informational/error
  messages logged as part of normal control flow, not exception handling.
- Exceptions: none.

**`DataIntegrity/DataIntegrityClasses/PandasInterpolation.py`**
- Logging: 1 `log()` call, immediately followed by raising `Semaphore_Data_Exception` — logs the
  detail, then converts into the custom exception type (one of the few spots where a log call and
  a raise are paired together).
- Exceptions: no try/except (raises directly, not catching anything).

**`DataIntegrity/DataIntegrityClasses/EnsemblePandasInterpolation.py`**
- Logging: imports `log` but **never calls it** — dead import (same pattern as `SEMAPHORE.py`).
- Exceptions: none.

Trivial: `DataIntegrity/__init__.py`.

## DataValidation

**`DataValidation/IDataValidation.py`** (interface + factory)
- Logging: none.
- Exceptions: same factory try/except pattern as `IDataIntegrity.py` — `(ModuleNotFoundError,
  AttributeError)` converted to `ImportError`, not logged.

**`DataValidation/DataValidationClasses/DateRangeValidation.py`**
- Logging: 6 `log_error()` calls — the heaviest `log_error` user outside `orchestrator.py`.
- Exceptions: no try/except at all — validation failures are reported purely by calling
  `log_error()` inline, not by raising anything.

**`DataValidation/DataValidationClasses/OverrideValidation.py`**
- Logging: none.
- Exceptions: no try/except; raises `Semaphore_Exception` directly when no matching validator is
  found for a label.

Trivial: `DataValidation/__init__.py`.

## ModelExecution

**`ModelExecution/dataGatherer.py`**
- Logging: 3 `log()` calls (informational).
- Exceptions: no try/except in this file — raises `Semaphore_Data_Exception` directly in two
  validation guard clauses (`OverrideValidation`/`DateRangeValidation` failures), left to
  propagate to the caller.

**`ModelExecution/dspecParser.py`**
- Logging: does **not** use the custom logger at all. Uses a raw `print()` for a "file not found"
  message immediately before raising `FileNotFoundError`.
- Exceptions: no try/except; raises directly.

**`ModelExecution/InputVectorBuilder.py`**
- Logging: 4 `log()` calls.
- Exceptions: no try/except; raises `Semaphore_Exception` directly for a missing key, and
  `NotImplementedError` for an unimplemented branch.

**`ModelExecution/IOutputHandler.py`** (interface + factory)
- Logging: none.
- Exceptions: factory wraps dynamic import in try/except; catches `Exception` broadly and
  converts to `ModuleNotFoundError`. Not logged.

**`ModelExecution/modelRunner.py`**
- Logging: 4 `log()` calls (informational, marking stages: load model, shape inputs, compute
  predictions, post-process).
- Exceptions: no try/except; raises `Semaphore_Exception` directly in three places (e.g. no model
  file found).

**`ModelExecution/OH_Classes/DefaultOutputHandler.py`**
- Logging: none.
- Exceptions: no try/except; raises `Semaphore_Exception` directly for shape-mismatch guard
  clauses.

Trivial (no logging, no exception handling): `ModelExecution/__init__.py`,
`ModelExecution/OH_Classes/MultiPackedFloat.py`, `ModelExecution/OH_Classes/OnePackedFloat.py`,
`ModelExecution/statistics.py`.

## PostProcessing

**`PostProcessing/IPostProcessing.py`** (interface + factory)
- Logging: none.
- Exceptions: same factory try/except pattern as the other `I*.py` factories — `(ModuleNotFoundError,
  AttributeError)` converted to `ImportError`, not logged.

**`PostProcessing/PostProcessingClasses/FourMaxMean.py`**
- Logging: 1 `log()` call (warning when input values are fewer than expected).
- Exceptions: none.

Trivial (no logging, no exception handling): `PostProcessing/__init__.py`,
`PostProcessing/PostProcessingClasses/ArithmeticOperation.py`,
`PostProcessing/PostProcessingClasses/Average.py`,
`PostProcessing/PostProcessingClasses/MagnoliaPredictionsPostProcess.py`,
`PostProcessing/PostProcessingClasses/ResolveVectorComponents.py`.

## SeriesProvider

**`SeriesProvider/SeriesProvider.py`**
- Logging: 6 `log()` calls.
- Exceptions: `__data_ingestion_query` catches `Exception` around the ingestion call, logs
  (message only, no traceback), then raises `Semaphore_Ingestion_Exception` — a converted
  re-raise. Separately, `request_output` has three parallel try/except blocks (one per dispatch
  case) that each catch a bare `TypeError` from a malformed kwargs call and convert it into
  `Semaphore_Exception` with a usage-hint message — not logged.

Trivial: `SeriesProvider/__init__.py`.

## SeriesStorage

**`SeriesStorage/ISeriesStorage.py`** (interface + factory)
- Logging: none.
- Exceptions: **no try/except** around its dynamic import in `series_storage_factory` — unlike
  every other `I*.py` factory in the codebase (`IDataIngestion`, `IDataIntegrity`,
  `IDataValidation`, `IOutputHandler`, `IPostProcessing`), a misconfigured
  `ISERIESSTORAGE_INSTANCE` env var here raises a raw, unwrapped `ModuleNotFoundError`/
  `AttributeError` instead of a friendlier converted exception.

**`SeriesStorage/SS_Classes/SQLAlchemyORM_Postgres.py`** (1218 lines — the largest file in `src`)
- Logging: only 3 `log()` calls in the entire file (contextual warnings, e.g. "no leadtime found").
- Exceptions: **no try/except anywhere in the file.** Several guard clauses raise `ValueError` or a
  bare `Exception` directly (e.g. wrong description type, malformed dataframe, DB engine not
  created) and always let them propagate uncaught to the caller.

Trivial: `SeriesStorage/__init__.py`.
