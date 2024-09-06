# frontendRunner.py
#----------------------------------
# Imports
import os
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from Frontend.GatherChartData import GatherChartData 
from utility import log
#need to create a log for frontend



def get_chart_data() -> str:
    chart_json_folder = './Frontend/ChartJson'
    
    for filename in os.listdir(chart_json_folder):

        filepath = os.path.join(chart_json_folder, filename)
        with open(filepath, 'r') as file:
            chart_data = json.load(file)
            
            #Seperate folder for frontend logs
            log(f"Processing chart Name: {chart_data['chartName']}")

            # Gather all the data needed for the chart
            gatherData = GatherChartData(chart_data)
            
            #Return a df instead
            final_df = gatherData.gather_chart_data

            #We will then write the df to the correct csv using the chartCSV name in the chartJSON
            #Add a try catch
            final_df.to_csv(f'{chart_data['chartCSV']}.csv', index=True)

               
    
    return json.dumps({"error": "Chart not found"})

if __name__ == '__main__':
    



