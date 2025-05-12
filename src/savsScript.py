import pandas as pd
import matplotlib.pyplot as plt
import os
 
# Download csv from sherlock prod
# Query data from the prod database & download as csv as well
# Use script to graph values against each other
# Do again for sherlock dev as well
 

 # Time generate = database
 # 
 
# Function to load a CSV file into a DataFrame
def load_csv_to_dataframe(file_path):
    """
    Reads a CSV file and converts it into a Pandas DataFrame.
 
    :param file_path: Path to the CSV file
    :return: Pandas DataFrame
    """
    try:
        df = pd.read_csv(file_path)
        print(f"CSV file '{file_path}' successfully loaded into a DataFrame.")
        print(df)
        return df
    except Exception as e:
        print(f"An error occurred while loading the CSV file '{file_path}': {e}")
        return None
 
# Function to plot air temperature predictions
def plot_air_temp_predictions(dataframes, labels, column_mapping, time_column_mapping):
    """
    Plots air temperature predictions from multiple DataFrames.
 
    :param dataframes: List of Pandas DataFrames
    :param labels: List of labels for the DataFrames
    :param column_mapping: Dictionary mapping labels to their air temperature column names
    :param time_column_mapping: Dictionary mapping labels to their time column names
    """
    plt.figure(figsize=(10, 6))
    for df, label in zip(dataframes, labels):
        air_temp_column = column_mapping.get(label, None)
        time_column = time_column_mapping.get(label, None)
        if air_temp_column and time_column and air_temp_column in df.columns and time_column in df.columns:
            # Ensure time column is datetime
            df[time_column] = pd.to_datetime(df[time_column], errors='coerce')
            df = df.dropna(subset=[time_column])  # Drop rows with invalid time
            df = df.sort_values(by=time_column)  # Sort by time column
            plt.plot(df[time_column], df[air_temp_column], label=label)
        else:
            print(f"Warning: Required columns ('{time_column}', '{air_temp_column}') not found in DataFrame for {label}")
 
    plt.title("Air Temperature Predictions")
    plt.xlabel("Time")
    plt.ylabel("Temperature")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()  # ensures everything fits well
    plt.savefig(os.path.abspath("dev_vs_db_comparison.png"), dpi=300)  # Save figure as PNG with high resolution
    plt.show()
 
 
# Function to compute 'time_db' from 'timeGenerated' and 'leadtime'
def compute_time_db(df):
    """
    Adds a 'time_db' column to the DataFrame by computing timeGenerated + leadtime.
 
    :param df: Pandas DataFrame
    :return: Modified DataFrame with 'time_db'
    """
    if 'timeGenerated' in df.columns and 'leadTime' in df.columns:
        # Convert timeGenerated to datetime
        df['timeGenerated'] = pd.to_datetime(df['timeGenerated'], errors='coerce')
 
        # Convert leadtime to timedelta
        def parse_leadtime(lt):
            try:
                return pd.to_timedelta(lt)
            except Exception:
                return pd.NaT
 
        df['leadtime_parsed'] = df['leadTime'].apply(parse_leadtime)
 
        # Compute time_db
        df['time_db'] = df['timeGenerated'] + df['leadtime_parsed']
    else:
        print("Missing 'timeGenerated' or 'leadtime' columns in Semaphore DB.")
    return df
 
 
# Function to align Flare and database predictions
def align_predictions(flare_df, db_df):
    """
    Aligns Flare predictions with database predictions based on timestamps.
 
    :param flare_df: DataFrame containing Flare predictions
    :param db_df: DataFrame containing database predictions
    :return: Two aligned DataFrames (Flare and DB)
    """
    # Ensure both time columns are datetime
    flare_df['Date'] = pd.to_datetime(flare_df['Date'], errors='coerce')
    db_df['time_db'] = pd.to_datetime(db_df['time_db'], errors='coerce')
 
    # Perform an inner join on the timestamps to align both datasets
    aligned_df = pd.merge(
        flare_df,
        db_df,
        left_on='Date',
        right_on='time_db',
        how='inner',
        suffixes=('_flare', '_db')
    )
 
    # Return the aligned Flare and DB DataFrames
    return aligned_df[['Date', 'Water Temperature Prediction']], aligned_df[['time_db', 'dataValue']]
 
 
# Example usage
if __name__ == "__main__":
    # File paths for the CSVs
    csv_files = [
        ("./src/flare_dev_data.csv", "Flare Dev"),
        ("./src/db_results.csv", "Semaphore DB")
    ]
 
    # Mapping of labels to their respective air temperature column names
    column_mapping = {
        "Flare Dev": "Water Temperature Prediction",
        "Semaphore DB": "dataValue"
    }
 
    # Mapping of labels to their respective time column names
    time_column_mapping = {
        "Flare Dev": "Date",
        "Semaphore DB": "time_db"
    }
 
    # Load data and store DataFrames and labels
    dataframes = []
    labels = []
    for file_path, label in csv_files:
        df = load_csv_to_dataframe(file_path)
        if df is not None:
            if label == "Semaphore DB":
                df = compute_time_db(df)
            dataframes.append(df)
            labels.append(label)
 
    # Align predictions
    if len(dataframes) == 2:
        flare_df, db_df = dataframes
        aligned_flare, aligned_db = align_predictions(flare_df, db_df)
 
        # Plot both Flare and DB predictions
        plt.figure(figsize=(10, 6))
        plt.plot(aligned_flare['Date'], aligned_flare['Water Temperature Prediction'], label='Flare Predictions', marker='o')
        plt.plot(aligned_db['time_db'], aligned_db['dataValue'], label='Semaphore DB Predictions', marker='x')
 
        # Add labels, legend, and title
        plt.title("Comparison of Flare and Semaphore DB Predictions")
        plt.xlabel("Time")
        plt.ylabel("Water Temperature")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
 
        # Save and show the plot
        plt.savefig(os.path.abspath("flare_vs_db_comparison.png"), dpi=300)
        plt.show()
    else:
        print("Failed to load and align both datasets.")