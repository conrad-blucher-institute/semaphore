from DBInterface import DBInterface
from sqlalchemy import insert, select


db = DBInterface()
db.create_engine("sqlite+pysqlite:///:memory:", False)
db.create_DB()

ld =    [
            {"code": "sandy", "displayName": "Sandy Cheeks"},
            {"code": "displayName", "displayName": "Sandy Cheeks"},
        ]

insertion = insert(db.s_ref_series).values(code="0000", displayName="Zeros", notes="")

with db.get_engine().connect() as conn:
    result = conn.execute(insert(db.s_ref_series), ld)
    conn.commit()



selection = select(db.s_ref_series)
with db.get_engine().connect() as conn:
    for row in conn.execute(selection):
        print(row)

db.drop_DB()
