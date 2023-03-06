SELECT * FROM s_prediction
WHERE 
    AIName= 'shallowNN_bhp_modelSaved' AND
    resultCode = 3 AND
    timeActualized BETWEEN [now] AND [now + 24 hours] 
;

SELECT * FROM s_data_point
WHERE 
    sLocationCode= [location] AND
    dataSourceCode= [datasource] AND
    seriesCode= [series] AND
    timeActualized BETWEEN [now] AND [now + 24 hours] 
;


floatID << SELECT id FROM s_ref_units WHERE displayName="float"
;

SELECT * FROM s_data_point
WHERE 
    unitsCode= floatID
;