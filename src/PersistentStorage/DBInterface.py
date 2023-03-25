      
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, Float, MetaData, UniqueConstraint, Engine

class DBInterface():
    
    def __init__(self) -> None:
        self.__create_schema()

    def create_DB(self) -> None:
        try:
            self._metadata_obj.create_all(self._engine)
        except AttributeError:
            raise Exception("An engine was requestied from DBInterface, but no engine has been created.")
        

    def drop_DB(self) -> None:
        try:
            self._metadata_obj.drop_all(self._engine)
        except AttributeError:
            raise Exception("An engine was requestied from DBInterface, but no engine has been created.")

    def create_engine(self, parmaString: str, echo:str ) -> None: #"sqlite+pysqlite:///:memory:"
        self._engine = create_engine(parmaString, echo=echo)
    
    def get_engine(self) -> Engine:
        try:
            return self._engine
        except AttributeError:
            raise Exception("An engine was requestied from DBInterface, but no engine has been created.")
        
    def get_metadata(self) -> MetaData:
        return self._metadata_obj

    def __create_schema(self) -> None:

        self._metadata_obj = MetaData()
        
        self.s_data_point = Table(
            "s_data_point",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),

            Column("timeActualized", DateTime, nullable=False),
            Column("timeAquired", DateTime, nullable=False),

            Column("dataValue", String(20), nullable=False),
            Column("unitsCode", String(10), nullable=False),

            Column("dataSourceCode", String(10), nullable=False),
            Column("sLocationCode", String(25), nullable=False),
            Column("seriesCode", String(10), nullable=False),
            Column("datumCode", String(10), nullable=True),
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),

            UniqueConstraint("timeActualized", "timeAquired", "dataValue", "unitsCode", "dataSourceCode", "sLocationCode", "seriesCode"),
        )

        self.s_prediction = Table(
            "s_prediction",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),

            Column("timeGenerated", DateTime, nullable=False),
            Column("leadTime", Float, nullable=False),

            Column("dataValue", String(20), nullable=False),
            Column("unitsCode", String(10), nullable=False),

            Column("resultCode", String(10), nullable=False),

            Column("dataSourceCode", String(10), nullable=False),
            Column("sLocationCode", String(25), nullable=False),
            Column("seriesCode", String(10), nullable=False),
            Column("datumCode", String(10), nullable=False),
            Column("latitude", String(16), nullable=False),
            Column("longitude", String(16), nullable=False),

            UniqueConstraint("timeGenerated", "leadTime", "dataValue", "unitsCode", "resultCode", "dataSourceCode", "sLocationCode", "seriesCode"),
        )

        self.s_locationCode_dataSourceLocationCode_mapping = Table(
            "s_locationCode_dataSourceLocationCode_mapping",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("sLocationCode", String(25), nullable=False),
            Column("dataSourceCode", String(10), nullable=False),
            Column("dataSourceLocationCode", String(255), nullable=False),
            Column("priorityOrder", Integer, nullable=False),

            UniqueConstraint("sLocationCode", "dataSourceCode", "dataSourceLocationCode", "priorityOrder"),
        )

        self.s_ref_slocation = Table(
            "s_ref_slocation",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(25), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_data_source = Table(
            "s_ref_data_source",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_series = Table(
            "s_ref_series",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_units = Table(
            "s_ref_units",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_datum = Table(
            "s_ref_datum",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )
        
        self.s_ref_resultCode = Table(
            "s_ref_resultCode",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )



