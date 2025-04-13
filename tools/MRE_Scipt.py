import json

HOURs = [3,6,12,18,24,30,36,42,48,54,60,66,72,78,84,90,96,102,108,114,120]


for HOUR in HOURs:
    dspec = {}

    dspec["dspecVersion"]  = "2.0"
    dspec["modelName"] = f"MRE_Bird-Island_Water-Temperature_{HOUR}hr"
    dspec["modelVersion"] = "1.0.0"
    dspec["author"] = "Christian Duff"
    dspec["modelFileName"] = f"./ColdStunning/Bird-Island_Water-Temperature_{HOUR}hr"

    dspec['timingInfo'] = {
        "active": True,
        "offset": 1200,
        "interval": 3600
    }


    dspec['outputInfo'] = {
        "outputMethod": "MultiPackedFloat",
        "leadTime": HOUR * 3600,
        "series": "pWaterTmp",
        "location": "SouthBirdIsland",
        "unit" : "celsius"
    }


    dspec['dependentSeries'] = [
        { 
            "_name": "Water Temp",
            "location": "SouthBirdIsland",
            "source": "LIGHTHOUSE",
            "series": "dWaterTmp",
            "unit": "celsius",
            "interval": 3600,
            "range": [0, -24],
            "outKey": "south-bird-island_water-temp_25",
            "dataIntegrityCall": {
                "call": "PandasInterpolation",
                "args": {
                    "limit":"3600",
                    "method":"linear",
                    "limit_area":"inside"    
                }
            }
        },
        { 
            "_name": "Air Temp",
            "location": "SouthBirdIsland",
            "source": "LIGHTHOUSE",
            "series": "dAirTmp",
            "unit": "celsius",
            "interval": 3600,
            "range": [0, -24],
            "outKey": "south-bird-island_air-temp_25",
            "dataIntegrityCall": {
                "call": "PandasInterpolation",
                "args": {
                    "limit":"3600",
                    "method":"linear",
                    "limit_area":"inside"    
                }
            }
        },
        { 
            "_name": "Ensemble Air Temp",
            "location": "SBirdIsland",
            "source": "TWC",
            "series": "pAirTemp",
            "unit": "celsius",
            "interval": 3600,
            "range": [HOUR + 6, 1],
            "outKey": f"ensemble-south-bird-island_predicted_air-temp_{HOUR + 6}",
            "dataIntegrityCall": {
                "call": "EnsemblePandasInterpolation",
                "args": {
                    "limit":"21600",
                    "method":"linear",
                    "limit_area":"inside"    
                }
            }
        }
    ]


    dspec['postProcessCall'] = []
    dspec['vectorOrder'] = [
        {
            "key": "south-bird-island_water-temp_25",
            "dType": "float"
        },
        {
            "key": "south-bird-island_air-temp_25",
            "dType": "float"
        },
        {
            "key": f"ensemble-south-bird-island_predicted_air-temp_{HOUR + 6}",
            "dType": "float",
            'indexes': [0, HOUR],
            "isMultipliedKey": True,
            "ensembleMemberCount": 100
        }

    ]


    json_txt = json.dumps(dspec, indent=4)

    with open(f'./data/dspec/ColdStunning/MRE_Bird-Island_Water-Temperature_{HOUR}hr.json', 'w') as f:
        f.write(json_txt)
        f.close()