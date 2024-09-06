# frontendRunner.py
#----------------------------------
# Imports
import os
import json
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from Frontend.GatherChartData import GatherChartData 
#need to create a log for frontend

# Initialize FastAPI and Redis
app = FastAPI()
cache = redis.Redis(host='localhost', port=6379, db=0)

def get_chart_data(chartName: str) -> str:
    chart_json_folder = './Frontend/ChartJson'

    # Check if the data is in the cache
    cached_data = cache.get(f"chart_{chartName}")
    if cached_data:
        print(f"Serving data for chart {chartName} from cache.")
        return cached_data.decode('utf-8')
    
    # Process data if not cached
    for filename in os.listdir(chart_json_folder):
        if filename.endswith('.json'):
            filepath = os.path.join(chart_json_folder, filename)
            with open(filepath, 'r') as file:
                chart_data = json.load(file)
                if chart_data['chartName'] == chartName:
                    print(f"Processing chart Name: {chart_data['chartName']}")

                    # Gather all the data needed for the chart
                    gatherData = GatherChartData(chart_data['chartSeries'])
                    
                    #Return a df instead
                    final_json = gatherData.gather_chart_data

                    #We will then write the df to the correct csv using the chartCSV name in the chartJSON
                    # Cache the final data for future requests
                    cache.set(f"chart_{chartName}", final_json, ex=360)  # Cache for 6 minutes
                    print(f"Data for chart {chartName} has been cached.")

                    return final_json
    
    return json.dumps({"error": "Chart not found"})

#The inital endpoint users will access and grab from cache if the data is there
@app.get('/{chartName}')
async def get_organized_data(chartName: str):
    return json.loads(get_chart_data(chartName))

#The endpoint frontend will access to update changes in the graph and the cache
@app.websocket("/ws/{chartName}")
async def websocket_endpoint(websocket: WebSocket, chartName: str):
    await websocket.accept()
    try:
        while True:
            # Send the cached or processed data
            await websocket.send_text(get_chart_data(chartName))
    except WebSocketDisconnect:
        print(f"Client disconnected from {chartName}")

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)#Open to change



