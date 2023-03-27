      
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, Float, Identity, MetaData

class DB():
    
    def __init__(self):
        self.metadata_obj = MetaData()
        
        self.s_data_point = Table(
            "s_data_point",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),

            Column("timeActualized", DateTime, primary_key=True),
            Column("timeAquired", DateTime, primary_key=True),

            Column("dataValue", String(20), primary_key=True),
            Column("unitsCode", String(6), primary_key=True),
            Column("dataSourceCode", String(10), primary_key=True),

            Column("sLocationCode", String(25), primary_key=True),
            Column("seriesCode", String(10), primary_key=True),
            Column("datumCode", String(10), nullable=True),
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),
        )

        self.s_prediction = Table(
            "s_prediction",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),

            Column("timeGenerated", DateTime, primary_key=True),
            Column("leadTime", Float),

            Column("dataValue", String(20), primary_key=True),
            Column("unitsCode", String(6), primary_key=True),

            Column("resultCode", String(6), primary_key=True),

            Column("sLocationCode", String(25), primary_key=True),
            Column("seriesCode", String(10), primary_key=True),
            Column("datumCode", String(10), nullable=True),
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),
        )

        self.s_locationCode_dataSourceLocationCode_mapping = Table(
            "s_locationCode_dataSourceLocationCode_mapping",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),
            
            Column("sLocationCode", String(25), primary_key=True),
            Column("dataSourceCode", String(10), primary_key=True),
            Column("dataSourceLocationCode", String(255), primary_key=True),
            Column("priorityOrder", Integer, primary_key=True),
        )

        self.s_ref_slocation = Table(
            "s_ref_slocation",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),
            Column("code", String(25), primary_key=True),
            Column("displayName", String(30), primary_key=True),
            Column("notes", String(250), nullable=True),
        )

        self.s_ref_data_source = Table(
            "s_ref_data_source",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),
            Column("code", String(10), primary_key=True),
            Column("displayName", String(30), primary_key=True),
            Column("notes", String(250), nullable=True),
        )

        self.s_ref_series = Table(
            "s_ref_series",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),
            Column("code", String(10), primary_key=True),
            Column("displayName", String(30), primary_key=True),
            Column("notes", String(250), nullable=True),
        )

        self.s_ref_units = Table(
            "s_ref_units",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),
            Column("code", String(6), primary_key=True),
            Column("displayName", String(30), primary_key=True),
            Column("notes", String(250), nullable=True),
        )

        self.s_ref_datum = Table(
            "s_ref_datum",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),
            Column("code", String(10), primary_key=True),
            Column("displayName", String(30), primary_key=True),
            Column("notes", String(250), nullable=True),
        )
        
        self.s_ref_resultCode = Table(
            "s_ref_resultCode",
            self.metadata_obj,

            Column("id", Integer, Identity(), primary_key=True),
            Column("code", String(6), primary_key=True),
            Column("displayName", String(30), primary_key=True),
            Column("notes", String(250), nullable=True),
        )

        

    def create_DB(self):
        self.metadata_obj.create_all(self.engine)

    def drop_DB(self):
        self.metadata_obj.drop_all(self.engine)

    def create_engine(self, parmaString): #"sqlite+pysqlite:///:memory:"
        self.engine = create_engine(parmaString, echo=True)



