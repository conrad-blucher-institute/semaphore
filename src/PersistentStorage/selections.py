from sqlalchemy import CursorResult, Select
from DBInterface import DBInterface



def dbSelection(dbInterface: DBInterface, stmt: Select) -> CursorResult:

    with dbInterface.get_engine().connect() as conn:
        result = conn.execute(stmt)

    return result
