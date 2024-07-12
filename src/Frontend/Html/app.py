# -*- coding: utf-8 -*-
#semaphoreRunner.py
#----------------------------------
# Created By : Anontiyae Beasley
# Created Date: 07/09/2024
# version 2.0
#----------------------------------
"""A file responsible for executing functions that gather data for each chart and then send the data to the corresponding chart.
 """ 
#----------------------------------
# 
#
#Imports
import os
import json
from flask import Flask, render_template
from DataIngestion.IDataIngestion import data_ingestion_factory
from Preprocessing.IPreProcessing import pre_processing_factory
import pandas as pd
from frontendUtility import create_series_and_time_description, inputs_to_dataframe

app = Flask(__name__)


@app.route('/')
def get_organized_data():
    # get the now time
    # Define the directory containing the JSON files
    chart_json_folder = './Frontend/ChartJson'

# Iterate through each file in the directory
    for filename in os.listdir(chart_json_folder):
        if filename.endswith('.json'):
            filepath = os.path.join(chart_json_folder, filename)
        
            # Read and parse the JSON file
            with open(filepath, 'r') as file:
                chart_data = json.load(file)
            
            # Print chart ID and Name
                print(f"Processing chart Name: {chart_data['chartName']}")
                combined_df = pd.DataFrame()
            # Iterate through the dependentSeries
                for dependent_serie in chart_data['dependentSeries']:
                    #Will need to make a "dataIngestion" class for Hohonu
                    series_description, time_description = create_series_and_time_description(dependent_serie)
                    data_ingestion_class = data_ingestion_factory(series_description)
                    series = data_ingestion_class.ingest_series(series_description, time_description)
                    df = inputs_to_dataframe(series.data)
                    preprocessClass = pre_processing_factory(dependent_serie['source'])
                    preprocessed_df = preprocessClass.pre_process_data(df)
                    # append to dataframe based on cbocp id
                    combined_df = pd.concat([combined_df, preprocessed_df])
                    
                # Convert combined DataFrame to JSON
                final_json = combined_df.to_json(orient='records')    
                
                # Use flask to send it to the html
                return render_template(f"{chart_data['chartName']}.html", final_json=final_json)

if __name__ == '__main__':
    
    app.run(debug=True)