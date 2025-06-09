import os
import subprocess
import zipfile
import sqlite3
import pandas as pd


# Extract

def get_retail_dataset(save_dir="data", zip_name="dataset.zip", filename="online_retail_II.csv"):
    """
    Downloads and extracts the Online Retail II dataset using curl.

    Parameters:
        save_dir (str): Directory where the dataset will be saved and extracted.
        zip_name (str): Name for the downloaded zip file.
        filename (str): Name of the expected CSV inside the zip.

    Returns:
        str: Path to the extracted CSV file.
    """
    os.makedirs(save_dir, exist_ok=True)
    zip_path = os.path.join(save_dir, zip_name)
    csv_path = os.path.join(save_dir, filename)

    if os.path.exists(csv_path):
        print(f"Dataset already exists at: {csv_path}")
        return csv_path

    print("Downloading dataset using curl...")
    try:
        subprocess.run([
            "curl", "-L", "-o", zip_path,
            "https://www.kaggle.com/api/v1/datasets/download/mashlyn/online-retail-ii-uci"
        ], check=True)
    except subprocess.CalledProcessError:
        raise RuntimeError("Curl download failed. Ensure you have access and a valid Kaggle session cookie.")

    print("Extracting dataset...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(save_dir)
    except zipfile.BadZipFile:
        raise RuntimeError("Failed to unzip the downloaded file. It may be corrupted or invalid.")

    if os.path.exists(csv_path):
        print(f"Dataset downloaded and extracted to: {csv_path}")
        return csv_path
    else:
        raise FileNotFoundError(f"Expected file {filename} not found after extraction.")


# Transform

def clean_data(df):
    print("Cleaning data...")
    df = df.copy()

    # Drop rows with any null values
    df.dropna(inplace=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Type conversion and column cleaning
    df["Customer ID"] = df["Customer ID"].astype(int)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["Description"] = df["Description"].str.strip()
    df["Country"] = df["Country"].str.strip()
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    
    # Filter rows
    df = df[df["Quantity"] > 0]
    df = df[df["Price"] > 0]
    df = df[df['Country'] == 'United Kingdom']

    return df


# Load

def load_to_sqlite(df, db_path="data/retail.db", table_name="retail_data"):
    print(f"Loading data into SQLite database at: {db_path}")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print("Data loaded successfully.")


# Main ETL Process

def run_etl():
    csv_file_path = get_retail_dataset()
    df_raw = pd.read_csv(csv_file_path, encoding="ISO-8859-1")
    df_clean = clean_data(df_raw)
    os.makedirs('data/processed/', exist_ok=True)
    df_clean.to_csv('data/processed/retail_cleaned.csv', index=False)
    load_to_sqlite(df_clean)

if __name__ == "__main__":
    run_etl()
