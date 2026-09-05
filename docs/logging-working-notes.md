# Logging in Semaphore — First-Pass Overview

Scope: `src/` only (excludes `src/API` and `src/tests`). This is a high-level survey meant to
support follow-up questions, not an exhaustive audit.

## 1. It's a custom logger, not a 3rd-party package or Python's `logging` module

There is no use of Python's built-in `logging` module and no 3rd-party logging library
(no `loguru`, `structlog`, etc.) anywhere in `src`. All logging goes through a small
hand-rolled utility in **`src/utility.py`**:

- `log(text, is_error=False, force_log=False)` — the core function. Prints to stdout and
  (conditionally) appends to a log file.
- `log_error(text)` — convenience wrapper: `log(text, is_error=True, force_log=True)`. Always written.
- `log_success(text)` — convenience wrapper: `log(text, is_error=False, force_log=True)`. Always written.

There are no other log levels (no debug/info/warning as distinct mechanisms — see §4).

## 2. Where logs go: `LogLocationDirector`

`LogLocationDirector` (in `utility.py`) is a singleton that holds the current log file path.

- Set once per run, in `orchestrator.py::run_semaphore`, right after a DSPEC is parsed:
  `LogLocationDirector().set_log_target_path(getenv('LOG_BASE_PATH'), model_name)`.
- Resulting path: `{LOG_BASE_PATH}/{model_name}/{year}_{month}_{model_name}.log` — i.e. **one log
  file per model, rotated monthly** (no size-based rotation, no cap on growth within a month).
- `LOG_BASE_PATH` is an env var (`/app/data/logs` in `.env.dist`). If it's unset, `log()` still
  prints to stdout but silently skips the file write (`log_file is None` check).
- Because the path is keyed on `model_name` and set inside the per-dspec loop in
  `run_semaphore`, logs from different models in the same batch run land in different files —
  but anything logged *before* the DSPEC is parsed (or if parsing itself fails) has nowhere to go
  but stdout.

## 3. Verbosity control: `VerbosityController`

Also a singleton in `utility.py`, with two flags: `verbose_mode` and `log_failures_only`.
Set from the CLI in `semaphoreRunner.py` via the `-v/--verbose` flag:

- Default (no `-v`): `log_failures_only = True` — plain `log()` calls are suppressed from both
  stdout and the file; only `log_error`/`log_success` (which pass `force_log=True`) get through.
- With `-v`: everything is logged.

Net effect: **routine/progress messages (`log(...)`) are silent by default**; only errors and
successes are guaranteed to be recorded. This is a meaningful behavior to know about if you're
trying to trace a run and only see error/success lines.

## 4. Consistency of log content

Centralized formatting is minimal: `utility.log()` prepends only a timestamp —
`f'{timeStamp}: {text}'` where `timeStamp = datetime.now(timezone.utc).strftime("%x %X")`.

- **Timestamp**: always UTC at the point of generation, but formatted with the locale-dependent
  `%x %X` (e.g. `09/05/26 14:23:01`), which **does not indicate UTC in the string itself** — a
  reader could reasonably mistake it for local time.
- **Module/function name**: not added automatically. It's entirely up to each call site, and
  practice is inconsistent:
  - Some prefix manually: `[Orchestrator]`, `[DataGatherer]`, `NDFD_EXP | fetch_predictions | ...`,
    `SQLAlchemyORM | select_specific_output | ...`
  - Many others have no identifying prefix at all (e.g. `log('Init DB Query...')` in
    `SeriesProvider.py`), so tracing a bare message back to its source means grepping the codebase.
- **Severity**: there's no level enum — severity is conveyed by which function was called
  (`log` vs `log_error` vs `log_success`) plus, inconsistently, free-text conventions developers
  typed into the message itself (`'WARNING:: ...'`, `'ERROR:: ...'`). Nothing enforces this.

**Usage is widespread** — the custom logger is used in ~20 files across `DataIngestion`,
`DataIntegrity`, `DataValidation`, `ModelExecution`, `PostProcessing`, `SeriesProvider`,
`SeriesStorage`, plus `orchestrator.py`/`discord.py`. `orchestrator.py` is the heaviest user (23
call sites).

**A few call sites bypass the logger entirely** and use raw `print()`:
`DataIngestion/DI_Classes/NDFD.py` (prints a URL and raw response text), `NDFD_EXP.py` (prints
exception internals), and `ModelExecution/dspecParser.py` (prints a "file not found" message).
These lines never reach the log file, only stdout.

## 5. Exception handling

Custom exception hierarchy lives in **`src/exceptions.py`**:

- `Semaphore_Exception`, `Semaphore_Data_Exception`, `Semaphore_Ingestion_Exception` — each
  carries an `error_code` and a `message`.
- Notably, **all three subclass `BaseException`, not `Exception`**. A bare `except Exception:`
  elsewhere in the codebase will *not* catch these. This pattern already appears in practice:
  `SeriesProvider.py::__data_ingestion_query` wraps ingestion in `except Exception as e:`, logs it
  (without a traceback), and then raises `Semaphore_Ingestion_Exception` — which is deliberately
  allowed to propagate past `Exception` handlers up to `orchestrator.py`.
- `Semaphore_Exception` and `Semaphore_Ingestion_Exception` also capture `sys.exc_info()` /
  `traceback.format_exc()` **at construction time** and bake it into `self.message`.
  `Semaphore_Exception.__init__` assumes an active exception context (`exc_tb.tb_frame` is
  accessed without a `None` check, unlike `Semaphore_Ingestion_Exception` which does guard it) —
  so constructing one outside an `except` block would raise `AttributeError`.

**Top-level handling** happens in `orchestrator.py::run_semaphore`, which wraps each DSPEC run in
a nested try/except: an inner `except Exception:` normalizes anything unanticipated into a plain
`Semaphore_Exception`, and an outer block catches the three custom exception types by name,
each logging (via `log_error`) a headline, the exception message, and
`traceback.format_exc()` — then routes to `__handle_failed_prediction` (Discord notification +
null-result DB write).

Net pattern: **exceptions tend to get logged twice** — once with just a message at the point
they're caught/re-raised deeper in the call stack (e.g. `SeriesProvider`), and again with a full
traceback at the `orchestrator` level. The exception classes' own traceback-capture adds a third
copy embedded in `self.message` in some cases.

## 6. Other error/notification paths

- `discord.py` sends failure/success notifications to Discord (gated by `DISCORD_NOTIFY` /
  `IS_DEV` env vars) and logs (once) if the notification itself fails to reach Discord.
- Both success and failure handlers in `orchestrator.py` wrap their DB-write logic in bare
  `except:` clauses that just call `log_error(...)` — no re-raise, no distinction between error
  types.

## Suggested follow-ups (not covered here)

- How `src/API` handles logging (likely different, since it wasn't in scope this pass).
- Whether log files ever get cleaned up / rotated out, or grow unbounded.
- Whether the double-logging-of-exceptions pattern is intentional or incidental.
- Whether `%x %X` should be replaced with an explicit ISO-8601 UTC format.

## Coding issues

Separate from the behavioral issues above, `utility.py` itself has some rough implementation
quality worth flagging:

- `log()` mixes three concerns (formatting, verbosity gating, stdout, file I/O) in one function
  with a confusing double-negative flag — `not verbosity.log_failures_only` on the
  `should_write_to_file` line means "log everything unless we're in failures-only mode," but it's
  easy to misread at a glance.
- Two singletons (`LogLocationDirector`, `VerbosityController`) are implemented with the same
  slightly unusual `hasattr(cls, 'instance')` pattern instead of something more idiomatic (e.g.
  `functools.lru_cache` or a plain module-level instance) — mutable global state that's easy to get
  out of sync in tests.
- `get_time_stamp()` is typed `-> None` but actually returns a string.
- Inconsistent indentation within the file — tabs at the top (`LogLocationDirector`), spaces from
  `VerbosityController` onward.
- The log file is opened and closed on every single `log()` call rather than holding a file
  handle open — fine at current log volume, but it's a full open/close syscall per line.
- Other weird coding decisions: the same `should_write_to_file` condition is checked in two
  separate `if` blocks back-to-back (lines 115–123) — one gating the `print(msg)` and one gating
  the file write — instead of being combined into a single `if` block, even though both branches
  execute under the identical condition.

