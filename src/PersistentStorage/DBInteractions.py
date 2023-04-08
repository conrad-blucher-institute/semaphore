# -*- coding: utf-8 -*-
#DBInteractions.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 3/26/2023
# version 1.0
#----------------------------------
""" This script holds a group of methods that insert or select into each of the different tables. Each requires a dictionary or a list 
disctionary to insert or a SQLAlchemy selection statment to select.
 """ 
#----------------------------------
# 
#
#Imports
from sqlalchemy import insert, Select, CursorResult
from DBInterface import DBInterface

def dbSelection(dbInterface: DBInterface, stmt: Select) -> CursorResult:
    """Runs a slection statment 
    ------
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        stmt: SQLAlchemy Select - The statement to run
    ------
    Returns:
        SQLAlchemy CursorResult
    """
    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(stmt)

    return result

def s_data_point_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_data_point
    ------
    Dictionary reference: {"timeActualized", "timeAquired", "dataValue", "unitsCode", "dataSourceCode", "sLocationCode", "seriesCode", (OP)"datumCode", (OP)"latitude", (OP)"longitude"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_data_point), values)
        conn.commit()

    return result


def s_prediction_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_predictions
    ------
    Dictionary reference: {"timeGenerated", "leadTime", "dataValue", "unitsCode", (OP)"resultCode", (OP)"resultCodeUnit", "dataSourceCode", "sLocationCode", "seriesCode", (OP)"datumCode", (OP)"latitude", (OP)"longitude"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_prediction), values)
        conn.commit()

    return result


def s_locationCode_dataSourceLocationCode_mapping_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_locationCode_dataSourceLocationCode_mapping
    ------
    Dictionary reference: {"dataSourceCode", "sLocationCode", "dataSourceLocationCode", "priorityOrder"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_locationCode_dataSourceLocationCode_mapping), values)
        conn.commit()

    return result


def s_ref_slocation_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_ref_slocation
    ------
    Dictionary reference: {"code", "displayName", (OP)"notes"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """
    
    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_slocation), values)
        conn.commit()

    return result


def s_ref_data_source_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_ref_data_source
    ------
    Dictionary reference: {"code", "displayName", (OP)"notes"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_data_source), values)
        conn.commit()

    return result


def s_ref_series_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_ref_series
    ------
    Dictionary reference: {"code", "displayName", (OP)"notes"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_series), values)
        conn.commit()

    return result


def s_ref_units_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_ref_units
    ------
    Dictionary reference: {"code", "displayName", (OP)"notes"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_units), values)
        conn.commit()

    return result

def s_ref_datum_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_ref_datum
    ------
    Dictionary reference: {"code", "displayName", (OP)"notes"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_datum), values)
        conn.commit()

    return result


def s_ref_resultCode_insert(dbInterface: DBInterface, values: dict | list[dict]) -> CursorResult:
    """Inserts a row or batch into s_ref_resultCode
    ------
    Dictionary reference: {"code", "displayName", (OP)"notes"}
    ------
    Parameters:
        dbInterface: DBInterface - The interface to fetch the engine from.
        values: dict | list[dict] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
    ------
    Returns:
        SQLAlchemy CursorResult
    """

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_resultCode), values)
        conn.commit()

    return result
