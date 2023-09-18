-- Create the "s_ref_slocation" table
CREATE TABLE s_ref_slocation
(
  id SERIAL PRIMARY KEY,
  code VARCHAR(25) NOT NULL,
  displayname VARCHAR(30) NOT NULL,
  notes VARCHAR(250),
  latitude VARCHAR(16) NOT NULL,
  longitude VARCHAR(16) NOT NULL
);

-- Create unique index for "s_ref_slocation"
CREATE UNIQUE INDEX unique_s_ref_slocation_code_displayname ON s_ref_slocation (code, displayname);

-- Create the "s_ref_data_source" table
CREATE TABLE s_ref_data_source
(
  id SERIAL PRIMARY KEY,
  code VARCHAR(10) NOT NULL,
  displayname VARCHAR(30) NOT NULL,
  notes VARCHAR(250)
);

-- Create unique index for "s_ref_data_source"
CREATE UNIQUE INDEX unique_s_ref_data_source_code_displayname ON s_ref_data_source (code, displayname);

-- Create the "s_ref_series" table
CREATE TABLE s_ref_series
(
  id SERIAL PRIMARY KEY,
  code VARCHAR(10) NOT NULL,
  displayname VARCHAR(30) NOT NULL,
  notes VARCHAR(250)
);

-- Create unique index for "s_ref_series"
CREATE UNIQUE INDEX unique_s_ref_series_code_displayname ON s_ref_series (code, displayname);

-- Create the "s_ref_units" table
CREATE TABLE s_ref_units
(
  id SERIAL PRIMARY KEY,
  code VARCHAR(10) NOT NULL,
  displayname VARCHAR(30) NOT NULL,
  notes VARCHAR(250)
);

-- Create unique index for "s_ref_units"
CREATE UNIQUE INDEX unique_s_ref_units_code_displayname ON s_ref_units (code, displayname);

-- Create the "s_ref_datum" table
CREATE TABLE s_ref_datum
(
  id SERIAL PRIMARY KEY,
  code VARCHAR(10) NOT NULL,
  displayname VARCHAR(30) NOT NULL,
  notes VARCHAR(250)
);

-- Create unique index for "s_ref_datum"
CREATE UNIQUE INDEX unique_s_ref_datum_code_displayname ON s_ref_datum (code, displayname);

-- Create the "s_ref_resultCodeUnits" table
CREATE TABLE s_ref_resultCodeUnits
(
  id SERIAL PRIMARY KEY,
  code VARCHAR(10) NOT NULL,
  displayname VARCHAR(30) NOT NULL,
  notes VARCHAR(250)
);

-- Create unique index for "s_ref_resultCodeUnits"
CREATE UNIQUE INDEX unique_s_ref_resultCodeUnits_code_displayname ON s_ref_resultCodeUnits (code, displayname);

-- Create the "s_data_point" table
CREATE TABLE s_data_point
(
  id SERIAL PRIMARY KEY,
  timeActualized TIMESTAMP NOT NULL,
  timeAquired TIMESTAMP NOT NULL,
  datavalue VARCHAR(20) NOT NULL,
  unitsCode VARCHAR(10) NOT NULL,
  interval INTEGER NOT NULL,
  dataSourceCode VARCHAR(10) NOT NULL,
  sLocationCode VARCHAR(25) NOT NULL,
  seriesCode VARCHAR(10) NOT NULL,
  datumCode VARCHAR(10),
  latitude VARCHAR(16),
  longitude VARCHAR(16)
);

-- Create unique index for "s_data_point"
CREATE UNIQUE INDEX unique_s_data_point ON s_data_point (timeActualized, datavalue, unitsCode, interval, dataSourceCode, sLocationCode, seriesCode);

-- Create the "s_prediction" table
CREATE TABLE s_prediction
(
  id SERIAL PRIMARY KEY,
  timeGenerated TIMESTAMP NOT NULL,
  leadTime FLOAT NOT NULL,
  datavalue VARCHAR(20) NOT NULL,
  unitsCode VARCHAR(10) NOT NULL,
  interval INTEGER NOT NULL,
  resultCode VARCHAR(10),
  dataSourceCode VARCHAR(10) NOT NULL,
  sLocationCode VARCHAR(25) NOT NULL,
  seriesCode VARCHAR(10) NOT NULL,
  datumCode VARCHAR(10),
  latitude VARCHAR(16),
  longitude VARCHAR(16)
);

-- Create unique index for "s_prediction"
CREATE UNIQUE INDEX unique_s_prediction ON s_prediction (timeGenerated, leadTime, datavalue, unitsCode, interval, dataSourceCode, sLocationCode, seriesCode);

-- Create the "s_prediction_output" table
CREATE TABLE s_prediction_output
(
  id SERIAL PRIMARY KEY,
  timeAquired TIMESTAMP NOT NULL,
  timeGenerated TIMESTAMP NOT NULL,
  leadTime FLOAT NOT NULL,
  modelName VARCHAR(25) NOT NULL,
  modelVersion VARCHAR(10) NOT NULL,
  datavalue VARCHAR(20) NOT NULL,
  unitsCode VARCHAR(10) NOT NULL,
  sLocationCode VARCHAR(25) NOT NULL,
  seriesCode VARCHAR(10) NOT NULL,
  datumCode VARCHAR(10)
);

-- Create unique index for "s_prediction_output"
CREATE UNIQUE INDEX unique_s_prediction_output ON s_prediction_output (timeGenerated, leadTime, modelName, modelVersion);

-- Create the "s_locationCode_dataSourceLocationCode_mapping" table
CREATE TABLE s_locationCode_dataSourceLocationCode_mapping
(
  id SERIAL PRIMARY KEY,
  sLocationCode VARCHAR(25) NOT NULL,
  dataSourceCode VARCHAR(10) NOT NULL,
  dataSourceLocationCode VARCHAR(255) NOT NULL,
  priorityOrder INTEGER NOT NULL
);

-- Create unique index for "s_locationCode_dataSourceLocationCode_mapping"
CREATE UNIQUE INDEX unique_s_locationCode_dataSourceLocationCode_mapping ON s_locationCode_dataSourceLocationCode_mapping (sLocationCode, dataSourceCode, dataSourceLocationCode, priorityOrder);

-- Add foreign key constraints
ALTER TABLE s_data_point ADD CONSTRAINT fk_s_data_point_unitsCode FOREIGN KEY (unitsCode) REFERENCES s_ref_units (code);
ALTER TABLE s_data_point ADD CONSTRAINT fk_s_data_point_dataSourceCode FOREIGN KEY (dataSourceCode) REFERENCES s_ref_data_source (code);
ALTER TABLE s_data_point ADD CONSTRAINT fk_s_data_point_sLocationCode FOREIGN KEY (sLocationCode) REFERENCES s_ref_slocation (code);
ALTER TABLE s_data_point ADD CONSTRAINT fk_s_data_point_seriesCode FOREIGN KEY (seriesCode) REFERENCES s_ref_series (code);
ALTER TABLE s_data_point ADD CONSTRAINT fk_s_data_point_datumCode FOREIGN KEY (datumCode) REFERENCES s_ref_datum (code);
ALTER TABLE s_prediction ADD CONSTRAINT fk_s_prediction_unitsCode FOREIGN KEY (unitsCode) REFERENCES s_ref_units (code);
ALTER TABLE s_prediction ADD CONSTRAINT fk_s_prediction_dataSourceCode FOREIGN KEY (dataSourceCode) REFERENCES s_ref_data_source (code);
ALTER TABLE s_prediction ADD CONSTRAINT fk_s_prediction_sLocationCode FOREIGN KEY (sLocationCode) REFERENCES s_ref_slocation (code);
ALTER TABLE s_prediction ADD CONSTRAINT fk_s_prediction_seriesCode FOREIGN KEY (seriesCode) REFERENCES s_ref_series (code);
ALTER TABLE s_prediction ADD CONSTRAINT fk_s_prediction_datumCode FOREIGN KEY (datumCode) REFERENCES s_ref_datum (code);
ALTER TABLE s_prediction_output ADD CONSTRAINT fk_s_prediction_output_unitsCode FOREIGN KEY (unitsCode) REFERENCES s_ref_units (code);
ALTER TABLE s_prediction_output ADD CONSTRAINT fk_s_prediction_output_sLocationCode FOREIGN KEY (sLocationCode) REFERENCES s_ref_slocation (code);
ALTER TABLE s_prediction_output ADD CONSTRAINT fk_s_prediction_output_seriesCode FOREIGN KEY (seriesCode) REFERENCES s_ref_series (code);
ALTER TABLE s_prediction_output ADD CONSTRAINT fk_s_prediction_output_datumCode FOREIGN KEY (datumCode) REFERENCES s_ref_datum (code);
ALTER TABLE s_locationCode_dataSourceLocationCode_mapping ADD CONSTRAINT fk_s_locationCode_dataSourceLocationCode_mapping_sLocationCode FOREIGN KEY (sLocationCode) REFERENCES s
