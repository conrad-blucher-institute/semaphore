# Semaphore Architecture

## Overview

Semaphore is a Python application that operationalizes AI models by gathering data, preparing model inputs, executing AI models, post-processing the results, and storing the output in a database.

The system is designed to make adding new models as simple as creating a new DSPEC configuration rather than modifying the application itself.

---

# High-Level Architecture

                   DSPEC + CLI
                        │
                        ▼
                SemaphoreRunner
                        │
                        ▼
                  Orchestrator
                        │
                        ▼
                  Data Gatherer
                        │
                        ▼
             Input Vector Builder
                        │
                        ▼
             AI Model (.keras/.h5)
                        │
                        ▼
                  Statistics
                        │
                        ▼
              PostgreSQL Database
---

# Execution Flow

When Semaphore runs a model, it performs the following steps:

0. Run the command
1. Load the DSPEC.
2. Download the required input data.
3. Store the data as `Series` objects.
4. Convert the data into model inputs.
5. Load the TensorFlow model.
6. Generate predictions.
8. Save the results using the configured output handler.

---

# Main Components

## Model Execution

Semaphore currently supports TensorFlow models stored as:

- `.keras`
- `.h5`

The appropriate model is loaded based on the DSPEC configuration.

## DSPEC

The DSPEC (Data Specification) describes everything needed to execute a model.

A DSPEC defines:

- Model location
- Required input data
- Timing information
- Output configuration
- Preprocessing
- Post-processing

Because Semaphore is configuration-driven, most new models can be added without modifying the application code.

---
## SemaphoreRunner

`SemaphoreRunner` is the command-line entry point for Semaphore. It parses user-provided command-line arguments, configures the runtime environment, and starts the execution process by passing control to the Orchestrator.

It supports running one or more DSPECs in a single execution and provides several optional flags for testing and debugging.

### Responsibilities

- Parse command-line arguments.
- Load environment variables from the `.env` file.
- Configure the logging verbosity.
- Parse an optional execution time for historical model runs.
- Support running Semaphore without storing results (`--toss`).
- Pass the requested DSPECs to the Orchestrator.

### Common Arguments

| Argument | Description |
|----------|-------------|
| `-d`, `--dspec` | One or more DSPEC files to execute. |
| `-p`, `--past` | Runs Semaphore using a past execution time (`YYYYMMDDHHMM`). |
| `-t`, `--toss` | Runs the model without saving predictions to the database. |
| `-v`, `--verbose` | Enables verbose logging. |

### Example

Run a single model:

```bash
python3 src/semaphoreRunner.py -d test_dspec.json
```

Run multiple models:

```bash
python3 src/semaphoreRunner.py \
-d model1.json model2.json model3.json
```

Run a model using a historical reference time:

```bash
python3 src/semaphoreRunner.py \
-d test_dspec.json \
-p 202601010000
```

## Orchestrator

The Orchestrator coordinates the execution of Semaphore. It is responsible for running each model from start to finish and ensuring that any errors are handled safely.

For each DSPEC, the Orchestrator performs the following steps:

1. Parses the DSPEC configuration.
2. Calculates the model's reference time.
3. Requests all required input data from the Data Gatherer.
4. Builds the model input vectors.
5. Executes the AI model.
6. Stores successful predictions in the database.
7. Computes output statistics (if enabled).
8. Sends Discord notifications (if configured).
9. Logs all execution details.

If an error occurs during execution, the Orchestrator catches the exception, logs the error, sends a notification, and inserts a null result into the database so the failed model run is still recorded.

---

## Data Gatherer

The Data Gatherer is responsible for preparing all of the input data required by a model before inference.

Given a DSPEC and a reference time, it performs the following steps:

1. Builds the required data requests from each dependent series.
2. Requests the data through the `SeriesProvider`.
3. Performs any configured data integrity processing.
4. Reindexes the data to the expected time interval.
5. Clips the data to only the points required by the model.
6. Validates the data to ensure it meets the model's requirements.
7. Executes any configured post-processing operations.
8. Returns a repository of `Series` objects to the Orchestrator.

If any required data is missing or fails validation, the Data Gatherer raises an exception and the model execution is stopped.

The returned data repository is then used by the Input Vector Builder to construct the model input vectors.

---

## Data Repository

The Data Repository is a collection of `Series` objects.

Each `Series` contains:

- Metadata
- Time information
- A pandas DataFrame

This repository is shared throughout model execution.

---


---

## Statistics Generation

The Statistics Engine computes summary statistics for model outputs before they are stored in the database or returned through the API.

For each set of predictions, it computes:

- Percentiles (1, 5, 10, 25, 50, 75, 90, 95, and 99)
- Minimum value
- Maximum value
- Mean
- Standard deviation

The 50th percentile represents the median and is calculated as part of the percentile calculations.

The Statistics Engine can also retrieve the latest stored statistics for one or more models from the database through the configured `SeriesStorage` implementation.

---

## Output Handlers

Output handlers determine where model predictions are written.

The default implementation stores results in PostgreSQL.

Additional output handlers can be created if new output destinations are needed.

---

# Scheduling

Models are executed automatically using cron.

`tools/init_cron.py`

reads every DSPEC and generates the cron schedule based on each model's:

- interval
- offset
- active status

Each cron job calls

```text
group_runner.py
```

which launches Semaphore with one or more DSPECs.

---

## Input Vector Builder

The Input Vector Builder is responsible for converting the processed data repository into the input vectors expected by the AI model.

Using the `vectorOrder` defined in the DSPEC, it determines which data should be included, the order it should appear in, and the required data types.

For each model execution, it performs the following steps:

1. Reads the `vectorOrder` from the DSPEC.
2. Retrieves each required `Series` from the data repository.
3. Selects the requested data points based on the configured indexes.
4. Casts each value to the required data type.
5. Concatenates the values into a single input vector.
6. Generates additional vectors for any series marked as multi-valued (such as ensemble members).
7. Returns a batch of input vectors for model inference.

The builder also verifies that the number of generated input vectors matches the expected number defined in the DSPEC.

---

# Database

Semaphore stores:

- model outputs
- metadata
- execution information

Database schema changes are handled through the migration system located in:

```text
DatabaseMigration/
```

---

# Docker Architecture

Semaphore runs as several Docker containers.

```text
                Docker Compose
                      │
      ┌───────────────┼───────────────┐
      │               │               │
      ▼               ▼               ▼
Semaphore Core   Semaphore API   PostgreSQL
```

Each container has its own responsibility.

- **Semaphore Core** executes models.
- **Semaphore API** serves prediction data.
- **PostgreSQL** stores Semaphore data.

---

# Adding a New Model

Adding a new model typically involves:

1. Add the trained model file.
2. Create a DSPEC.
3. Verify the input data exists.
4. Test the model.
5. Deploy the updated cron schedule.

Most models can be added without modifying Semaphore's core logic.

---

# Directory Overview

```text
src/
│
├── API/
├── DataGatherer/
├── DataClasses/
├── Database/
├── DatabaseMigration/
├── OutputHandler/
├── PostProcessing/
├── tools/
└── semaphoreRunner.py
```

Each directory contains one major subsystem of Semaphore.