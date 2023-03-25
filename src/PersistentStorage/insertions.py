from sqlalchemy import insert, CursorResult
from DBInterface import DBInterface

def s_data_point_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_data_point), values)
        conn.commit()

    return result


def s_prediction_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_prediction), values)
        conn.commit()

    return result


def s_locationCode_dataSourceLocationCode_mapping_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_locationCode_dataSourceLocationCode_mapping), values)
        conn.commit()

    return result


def s_ref_slocation_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_slocation), values)
        conn.commit()

    return result


def s_ref_data_source_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_data_source), values)
        conn.commit()

    return result


def s_ref_series_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_series), values)
        conn.commit()

    return result


def s_ref_units_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_units), values)
        conn.commit()

    return result

def s_ref_datum_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_datum), values)
        conn.commit()

    return result


def s_ref_resultCode_insert(dbInterface: DBInterface, values: dict | list(dict)) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(insert(dbInterface.s_ref_resultCode), values)
        conn.commit()

    return result