ALTER TABLE outputs 
ALTER COLUMN "dataSeries" TYPE VARCHAR(25)
ALTER TABLE inputs 
ALTER COLUMN "dataSeries" TYPE VARCHAR(25)
ALTER TABLE ref_dataSeries 
ALTER COLUMN "code" TYPE VARCHAR(25)

---03/19/2024

INSERT INTO public."ref_dataDatum"("code","displayName","notes")
VALUES
('NAVD','NAVD','North American Vertical Datum of 1988'),
('NA','NA','Not Applicable')
ON CONFLICT DO NOTHING;

INSERT INTO public."ref_dataLocation"("code","displayName","notes","latitude","longitude")
VALUES
('Freeport','Freeport Texas','Freeport Texas','27.908','95.343'),
('Aransas','Aransas Pass','Aransas Pass','27.83444444','-97.06777778')
ON CONFLICT DO NOTHING;

INSERT INTO public."ref_dataSeries"("code","displayName","notes")
VALUES
('dXWnCmp000D','Wind Speed X','Actual Wind Speed, X Component, Offset by xxD where 00D would be minus 0 degrees'),
('dYWnCmp000D','Wind Speed Y','Actual Wind Speed, Y Component, Offset by xxD where 00D would be minus 0 degrees'),
('pXWnCmp000D','Wind Speed X','Predictive Wind Speed, X Component, Offset by xxD where 00D would be minus 0 degrees'),
('pYWnCmp000D','Wind Speed Y','Predictive Wind Speed, Y Component, Offset by xxD where 00D would be minus 0 degrees'),
('pSurge','Surge','Predictive Water Surge'),
('pWl','Water Level','Predictive water height'),
('d_48h_4mm_WVHT','4 max mean WVHT','Mean of the four max of wave height of the last 48 hours'),
('d_24h_4mm_WVHT','4 max mean WVHT','Mean of the four max of wave height of the last 24 hours'),
('d_12h_4mm_WVHT','4 max mean WVHT','Mean of the four max of wave height of the last 12 hours'),
('d_48h_4mm_DPD','4 max mean DPD','Mean of the four max of wave Dominant Wave Period of the last 48 hours'),
('d_24h_4mm_DPD','4 max mean DPD','Mean of the four max of wave Dominant Wave Period of the last 24 hours'), 
('d_12h_4mm_DPD','4 max mean DPD','Mean of the four max of wave Dominant Wave Period of the last 12 hours'), 
('d_48h_4mm_APD','4 max mean APD','Mean of the four max of the Average Period of the last 48 hours'), 
('d_24h_4mm_APD','4 max mean APD','Mean of the four max of the Average Period of the last 24 hours'), 
('d_12h_4mm_APD','4 max mean APD','Mean of the four max of the Average Period of the last 12 hours'), 
('d_48h_4mm_wl','4 max mean water level','Mean of the four max of the Water Level of the last 48 hours'),
('d_24h_4mm_wl','4 max mean water level','Mean of the four max of the Water Level of the last 24 hours'),
('d_12h_4mm_wl','4 max mean water level','Mean of the four max of the Water Level of the last 12 hours'),
('pInundation', 'Predictive Inundation','Inundation is how far the water comes up on land in meters'),
('dWnDir','Wind Direction','Wind Direction'),
('dWnSpd','Wind Speed','Wind Speed')
ON CONFLICT DO NOTHING;

INSERT INTO public."ref_dataSource"("code","displayName","notes")
VALUES
('NDBC','NDBC','National Data Buoy Center')
ON CONFLICT DO NOTHING;

INSERT INTO public."ref_dataUnit"("code","displayName","notes")
VALUES
('seconds','Seconds',''),
('mps','Meters per Seconds',''),
('degrees','Degrees of Rotation','')
ON CONFLICT DO NOTHING;

INSERT INTO public."dataLocation_dataSource_mapping"("dataLocationCode","dataSourceCode","dataSourceLocationCode","priorityOrder")
VALUES
('Freeport','NDBC','42019','0'),
('Freeport','NOAATANDC','8772471','0'),
('Aransas','NOAATANDC','8775241','0'),
('Aransas','NDFD','0','0')
ON CONFLICT DO NOTHING;

---03/20/2024

INSERT INTO public."ref_dataLocation"("code","displayName","notes","latitude","longitude")
VALUES
('PortIsabel', 'Port Isabel','Port Isabel TX','26.05194444','-97.20250000'),
('NorthJetty','North Jetty','Galveston Bay Entrance North Jetty TX','29.35111111','-94.71805556'),
('Rockport', 'Rockport Texas', 'Rockport Texas', '28.01750000','-97.03555556')
ON CONFLICT DO NOTHING;

INSERT INTO public."dataLocation_dataSource_mapping"("dataLocationCode","dataSourceCode","dataSourceLocationCode","priorityOrder")
VALUES
('PortIsabel','NOAATANDC','8779770','0'),
('PortIsabel','NDFD','0','0'),
('NorthJetty','NOAATANDC','8771341','0'),
('NorthJetty','NDFD','0','0'),
('Rockport','NOAATANDC','8774770','0'),
('Rockport','NDFD','0','0')
ON CONFLICT DO NOTHING;

INSERT INTO public."ref_dataSeries"("code","displayName","notes")
VALUES
('pWnDir','Predicted Wind Direction','Wind Direction'),
('pWnSpd','Predicted Wind Speed','Wind Speed')
ON CONFLICT DO NOTHING;