# -*- coding: utf-8 -*-
# test_Semaphore.py
# -------------------------------
# Created By : Anointiyae Beasley
# Created Date: 2026-06-26
# version 1.0
# -------------------------------
"""
Unit tests to test a successful run of each model group.

High-level flow:
1) Mock ingestion data for each model group.
2) Feed to model execution.
"""

import sys
sys.path.append("/app/src")

from datetime import datetime, timedelta, timezone
from pathlib import Path
import traceback
import pandas as pd
import pytest
import random
from unittest.mock import MagicMock, patch

from src.orchestrator import Orchestrator
from src.DataClasses import (
    Series,
    SeriesDescription,
    TimeDescription,
    get_input_dataFrame,
)


DATA_DIR = Path("/app/data/dspec")

MODEL_GROUPS = [
    "ColdStunning",
    "CRPS",
    "Inundation",
    #"Magnolia",
    "Surge",
    "VirginiaKey",
]


def make_series(index, reference_time, is_multi, input_vector_count) -> Series:
    """
    Creates a synthetic Series matching the requested dependent series.
    """

    from_offset, to_offset = index
    interval = timedelta(seconds=3600)

    start_time = reference_time + from_offset * interval

    rows = abs(to_offset - from_offset) + 1

    description = SeriesDescription(
        dataSource=random.choice(["Source1", "Source2", "Source3", "Source4", "Source5"]),
        dataSeries=random.choice(["Series1", "Series2", "Series3", "Series4", "Series5"]),
        dataLocation=random.choice(["Location1", "Location2", "Location3", "Location4", "Location5"]),
        dataDatum=random.choice(["Datum1", "Datum2", "Datum3", "Datum4", "Datum5"]),
        dataIntegrityDescription=None,
        verificationOverride=None,
    )

    time_description = TimeDescription(
        fromDateTime=start_time,
        toDateTime=start_time + interval * (rows - 1),
        interval=interval,
        stalenessOffset=None,
    )

    series = Series(description, time_description)

    df = get_input_dataFrame()

    times = pd.date_range(
        start=start_time,
        periods=rows,
        freq=interval,
        tz="UTC",
    )

    if is_multi:
        df["dataValue"] = [
            [str(float(i + j)) for j in range(input_vector_count)]
            for i in range(rows)
        ]
    else:
        df["dataValue"] = [str(float(i)) for i in range(rows)]
    df["timeVerified"] = times
    df["timeGenerated"] = times
    
    df["dataUnit"] = random.choice(["Unit1", "Unit2", "Unit3", "Unit4", "Unit5"])
    df["longitude"] = "-97.3181000"
    df["latitude"] = "27.4847000"

    series.dataFrame = df

    return series


def build_mock_repository(dspec, reference_time):
    """
    Replacement for DataGatherer.get_data_repository().
    Builds synthetic input data for every dependent series
    requested by the DSPEC.
    """

    repository = {}

    vo = dspec.orderedVector

    for key, index in zip(vo.keys, vo.indexes) :
        print(f"==================={key}: {index}====================")
        if index == (None, None):
            suffix = key.rsplit("_", 1)[1]
            if suffix.isdigit():
                number = int(suffix)
                index = (0, number - 1)
            else:
                raise ValueError(f"Cannot infer index range from key '{key}'")

        repository[key] = make_series(
            index,
            reference_time,
            is_multi=(key in vo.multipliedKeys),
            input_vector_count=dspec.outputInfo.expectedOutputShape.inputVectorCount
        )

    return repository


def get_dspec_paths(model_group):
    """
    Returns every DSPEC inside a model group.
    """

    folder = DATA_DIR / model_group

    return sorted(
        str(path)
        for path in folder.rglob("*.json")
    )


@pytest.mark.parametrize("model_group", MODEL_GROUPS)
def test_model_group_runs(model_group):

    execution_time = datetime(
        2026,
        6,
        28,
        18,
        0,
        tzinfo=timezone.utc,
    )

    orchestrator = Orchestrator()
    with patch.object( orchestrator, "_Orchestrator__handle_successful_prediction") as mock_success:

        orchestrator.dataGatherer.get_data_repository = MagicMock(
            side_effect=build_mock_repository
        )

        dspecs = [
            path
            for path in get_dspec_paths(model_group)
            if not path.endswith("comment.json")
        ]
        assert dspecs, f"No DSPECs found for {model_group}"

        try:
            orchestrator.run_semaphore(
            dspecs,
            executionTime=execution_time,
            toss=True,
        )
        except Exception:
            traceback.print_exc()
            raise
        
        

        assert mock_success.call_count == len(dspecs)