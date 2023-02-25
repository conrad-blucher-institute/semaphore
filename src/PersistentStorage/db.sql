
---------------------------------------------
-- Recommended schema:  effecient and flexible for insertion and retrieval from an API standpoint and for human readers
-- store individual measurement or prediction for a given timestamp, location, environmental variable type and data source


-- this table stores the actual data values as retrieved or received 
CREATE TABLE nc_metocean_data_point (
    id int primary key not null,            -- DB assigned ID
    valueTimestamp varchar(20) not null,    -- the timestamp for the value in GMT
    dataValue varchar(10) not null,         -- actual value for the data points
    unitsCode varchar(6) not null,          -- unit for the value (e.g., Farheneit, Celcius, m/s, etc)
    ncLocationCode varchar(25) not null,    -- neural coast specific ID for the location
    seriesCode varchar(10) not null,        -- the code for the type of measurement or prediction.  E.g., wdir, wspeed, wlevel, wtemp, offswl, harmonic, etc..
    dataSourceCode varchar(10) not null,    -- the code for the source from which the value was obtained:  e.g, NOAA, NDFD, TCOON, etc.       -- the time at which we ingested/computed the value Unix Time
    waterLevelDatum varchar(10),            -- the datum for water-level related series (e.g., water-levle, harmonic) 
    latitude int,                           -- the latitude of the location for which the data point is provided
    longitude int,                          -- the longitude of the location for which the data point is provided
    _dateCreated varchar(20) not null,       -- metadata tells us when the row as insterted
    _dateUpdated varchar(20),               -- metadata tells us when/whether the row was updated
    _createdBy varchar(20) not null,        -- metadata tells us who/what created the row
    _updatedBy varchar(20),                 -- metaata tells us who/what updated the row
    CONSTRAINT alt_key UNIQUE (valueTimestamp, seriesCode, ncLocationCode, dataSourceCode)
    );


-- this table provides a way to specify a location code that is specific to a datasource and is needed
-- when retrieving data from that source.  For example, when retrieving data from NOAA Tides and Currents (TaC)
-- we must use TaC's specific location code (not long and lat). So, in this table, we store the mappings
-- between the CBI specific location code (e.g., 'AransasPassChannel') and the TaC location code for that
-- station (e.g., '8775241').  So the overall row for that station and TaC would look like this:
-- 'AransasPassChannel', 'NOAA_TAC', '8775241', 1, datetime('now'), 'nc-metocean-depl'
CREATE TABLE nc_locationCode_dataSourceLocationCode_mapping (
    id int primary key not null,
    ncLocationCode varchar(25) not null,
    dataSourceCode varchar(10) not null,
    dataSourceLocationCode varchar(10) not null,
    priorityOrder int not null,             -- supoprts providing multiple mappings for the same location and pririoritize them
    _dateCreated varchar(20) not null,       -- metadata tells us when the row as insterted
    _dateUpdated varchar(20),               -- metadata tells us when/whether the row was updated
    _createdBy varchar(20) not null,        -- metadata tells us who/what created the row
    _updatedBy varchar(20),                 -- metaata tells us who/what updated the row
    CONSTRAINT alt_key UNIQUE (ncLocationCode, dataSourceCode, priorityOrder)
);

-- the following are reference tables mostly used for the human reader - they are not needed but nice to have. For example, for coders 
--  who may be looking for valid values to use for type code and data source code, or as a quick reference of what location a CBI ID refers to
CREATE TABLE nc_ref_metocean_location (
    id int primary key not null,
    ncLocationCode varchar(10) not null,
    displayName varchar(30),
    notes varchar(250),
    _dateCreated varchar(20) not null,       -- metadata tells us when the row as insterted
    _dateUpdated varchar(20),               -- metadata tells us when/whether the row was updated
    _createdBy varchar(20) not null,        -- metadata tells us who/what created the row
    _updatedBy varchar(20),                 -- metaata tells us who/what updated the row
    CONSTRAINT alt_key UNIQUE (ncLocationCode)
);

CREATE TABLE nc_ref_metocean_data_source(
    id int primary key not null,
    code char(10) not null,
    displayName varchar(30),
    notes varchar(250),
    _dateCreated varchar(20) not null,       -- metadata tells us when the row as insterted
    _dateUpdated varchar(20),               -- metadata tells us when/whether the row was updated
    _createdBy varchar(20) not null,        -- metadata tells us who/what created the row
    _updatedBy varchar(20),                 -- metaata tells us who/what updated the row
    CONSTRAINT alt_key UNIQUE (code)
);

CREATE TABLE nc_ref_metocean_series(
    id int primary key not null,
    code char(10) not null,
    displayName varchar(30),
    recommendedUnitsCode varchar (6) not null,
    notes varchar(250),
    _dateCreated varchar(20) not null,       -- metadata tells us when the row as insterted
    _dateUpdated varchar(20),               -- metadata tells us when/whether the row was updated
    _createdBy varchar(20) not null,        -- metadata tells us who/what created the row
    _updatedBy varchar(20),                 -- metaata tells us who/what updated the row
    CONSTRAINT alt_key UNIQUE (code)
);

CREATE TABLE nc_ref_metocean_units(
    id int primary key not null,
    code char(10) not null,
    displayName varchar(30),
    notes varchar(250),
    _dateCreated varchar(20) not null,       -- metadata tells us when the row as insterted
    _dateUpdated varchar(20),               -- metadata tells us when/whether the row was updated
    _createdBy varchar(20) not null,        -- metadata tells us who/what created the row
    _updatedBy varchar(20),                 -- metaata tells us who/what updated the row
    CONSTRAINT alt_key UNIQUE (code)
);