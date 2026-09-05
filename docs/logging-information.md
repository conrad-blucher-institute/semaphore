# File purpose

This file documents how logging is currently implemented in Semaphore for the purpose of comming up with recommendations for improving it and ensureing that things get logged more consistantly.


# Current logging pattern

Most logging in Semaphore is done through a custom set of logging functions that are defined in the `src/utility.py` module. This module provides a main logging function called - approriately enough - "log" and a couple of other methods to log things explicitely as errors or success. There is no defined log levels and the specific methods for error and success do not log a level when logging. The is_error flag drives (among other things) whether a message to logged to a file or just to stdoutput.

- `log(text, is_error=False, force_log=False)` — the core function. Prints to stdout and
  (conditionally) appends to a log file.
- `log_error(text)` — convenience wrapper: `log(text, is_error=True, force_log=True)`. Always written.
- `log_success(text)` — convenience wrapper: `log(text, is_error=False, force_log=True)`. Always written.


# Logging requirements

## Target logging behavior

1. a centralize logger that consistantly logs the following information:
* timestamp in UTC time, formatted to make it clear that it is UTC
* the name of the module that requested the logging
* when available, the name of the model that is being executed 
* when available, the name of the series that is being manipulated/for which code is being executed (e.g., ingestion, data integrity and post processing classes should provide a list of series with their roles - e.g., in vs out)
Ideally the logger uses contextvars or something similar to let the executing code pass in some of these data points
2. a consitent approach based on best practices for all modules to use for logging: when to log, at what level, when not to log. information to provide in log messages, etc.
3. Logging from any module should be easy and straightforward and as standard as we can make it

Let's not reinvent the wheel on that one. We should use the build-in logging module and wrap it with some extra functionality that satisfy the above requirement

## Target Exception raising and handling behavior

1. The key task of Semaphoer is to execute a Model Run.  Hence exception raising should all happen around that one step.  


# notes - to be deleted

**Usage is widespread** — the custom logger is used in ~20 files across `DataIngestion`,
`DataIntegrity`, `DataValidation`, `ModelExecution`, `PostProcessing`, `SeriesProvider`,
`SeriesStorage`, plus `orchestrator.py`/`discord.py`. `orchestrator.py` is the heaviest user (23
call sites).

## Where logs go: `LogLocationDirector`

`LogLocationDirector` (in `utility.py`) is a singleton that holds the current log file path.

- Set once per run, in `orchestrator.py::run_semaphore`, right after a DSPEC is parsed:
  `LogLocationDirector().set_log_target_path(getenv('LOG_BASE_PATH'), model_name)`.
- Resulting path: `{LOG_BASE_PATH}/{model_name}/{year}_{month}_{model_name}.log` — i.e. one log
  file per model, rotated monthly (no size-based rotation, no cap on growth within a month).
- `LOG_BASE_PATH` is an env var (`/app/data/logs` in `.env.dist`). If it's unset, `log()` still
  prints to stdout but silently skips the file write (`log_file is None` check).
- Because the path is keyed on `model_name` and set inside the per-dspec loop in
  `run_semaphore`, logs from different models in the same batch run land in different files.

## Verbosity control: `VerbosityController`

Also a singleton in `utility.py`, with two flags: `verbose_mode` and `log_failures_only`.
Set from the CLI in `semaphoreRunner.py` via the `-v/--verbose` flag:

- Default (no `-v`): `log_failures_only = True` — plain `log()` calls are suppressed from both
  stdout and the file; only `log_error`/`log_success` (which pass `force_log=True`) get through.
- With `-v`: everything is logged.

Net effect: routine/progress messages (`log(...)`) are silent by default; only errors and
successes are guaranteed to be recorded.

## Log content format

Centralized formatting is minimal: `utility.log()` prepends only a timestamp —
`f'{timeStamp}: {text}'` where `timeStamp = datetime.now(timezone.utc).strftime("%x %X")`.

- **Timestamp**: always UTC at the point of generation, formatted with `%x %X`.
- **Module/function name**: not added automatically — left entirely to each call site (see
  Issues below for how consistently that's done).
- **Severity**: no level enum — conveyed by which function was called (`log` vs `log_error` vs
  `log_success`), plus free-text conventions typed into the message itself.

## Exception handling

Custom exception hierarchy lives in **`src/exceptions.py`**:

- `Semaphore_Exception`, `Semaphore_Data_Exception`, `Semaphore_Ingestion_Exception` — each
  carries an `error_code` and a `message`.
- `Semaphore_Exception` and `Semaphore_Ingestion_Exception` capture `sys.exc_info()` /
  `traceback.format_exc()` at construction time and bake it into `self.message`.

**Top-level handling** happens in `orchestrator.py::run_semaphore`, which wraps each DSPEC run in
a nested try/except: an inner `except Exception:` normalizes anything unanticipated into a plain
`Semaphore_Exception`, and an outer block catches the three custom exception types by name, each
logging (via `log_error`) a headline, the exception message, and `traceback.format_exc()` — then
routes to `__handle_failed_prediction` (Discord notification + null-result DB write).

## Other error/notification paths

- `discord.py` sends failure/success notifications to Discord (gated by `DISCORD_NOTIFY` /
  `IS_DEV` env vars) and logs (once) if the notification itself fails to reach Discord.
- Both success and failure handlers in `orchestrator.py` wrap their DB-write logic in bare
  `except:` clauses that just call `log_error(...)` — no re-raise, no distinction between error
  types.

# Issues

- **Timestamp format is ambiguous.** `%x %X` (e.g. `09/05/26 14:23:01`) doesn't indicate UTC in
  the string itself — a reader could reasonably mistake it for local time.
- **Module/function prefixes are inconsistent.** Some call sites prefix manually
  (`[Orchestrator]`, `[DataGatherer]`, `NDFD_EXP | fetch_predictions | ...`,
  `SQLAlchemyORM | select_specific_output | ...`); many others have no identifying prefix at all
  (e.g. `log('Init DB Query...')` in `SeriesProvider.py`), so tracing a bare message back to its
  source means grepping the codebase.
- **Severity has no enforced convention.** `'WARNING:: ...'` / `'ERROR:: ...'` text prefixes are
  typed manually by developers and nothing enforces their use or consistency.
- **A few call sites bypass the logger entirely** via raw `print()`:
  `DataIngestion/DI_Classes/NDFD.py` (prints a URL and raw response text), `NDFD_EXP.py` (prints
  exception internals), and `ModelExecution/dspecParser.py` (prints a "file not found" message).
  These lines never reach the log file, only stdout.
- **Custom exceptions subclass `BaseException`, not `Exception`.** A bare `except Exception:`
  elsewhere in the codebase will *not* catch `Semaphore_Exception`/`Semaphore_Data_Exception`/
  `Semaphore_Ingestion_Exception`. This is already relied on in practice:
  `SeriesProvider.py::__data_ingestion_query` wraps ingestion in `except Exception as e:`, logs it
  (without a traceback), then raises `Semaphore_Ingestion_Exception`, which is deliberately allowed
  to propagate past `Exception` handlers up to `orchestrator.py`. Worth confirming this is
  intentional rather than an accident of inheriting from the wrong base class.
- **`Semaphore_Exception.__init__` assumes an active exception context.** It accesses
  `exc_tb.tb_frame` without a `None` check (unlike `Semaphore_Ingestion_Exception`, which does
  guard it) — constructing one outside an `except` block would raise `AttributeError`.
- **Exceptions tend to get logged multiple times.** Once with just a message at the point they're
  caught/re-raised deeper in the call stack (e.g. `SeriesProvider`), again with a full traceback at
  the `orchestrator` level via `log_error`, and a third copy embedded in `self.message` from the
  exception class's own traceback capture in some cases.
- **Logging gaps before a log file target exists.** Anything logged before a DSPEC is parsed (or
  if parsing itself fails) has nowhere to go but stdout, since `LogLocationDirector` isn't set yet.
- Not yet investigated: how `src/API` handles logging (out of scope this pass), whether log files
  ever get cleaned up or grow unbounded, and whether the double-logging-of-exceptions pattern is
  intentional.
