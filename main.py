import argparse
import os
import json
import datetime
from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import load_data

def main():
    """
    Main function to run the fashion data ingestion pipeline.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fashion Data Ingestion Pipeline')
    parser.add_argument('--csv-only', action='store_true', help='Save to CSV file only')
    parser.add_argument('--output-path', type=str, help='Output path for CSV file')
    parser.add_argument('--pg-config', type=str, help='PostgreSQL configuration file path (JSON)')
    parser.add_argument('--gs-creds', type=str, default='google-sheets-api.json', 
                      help='Google Sheets credentials file path')
    
    args = parser.parse_args()
    
    # Extract raw data
    print("Starting data extraction...")
    raw_data = extract_data()
    
    # Transform the data
    print("Transforming data...")
    df = transform_data(raw_data)
    
    # Configure loading options
    load_to_pg = not args.csv_only
    load_to_gsheets = not args.csv_only
    
    # Load PostgreSQL config if provided
    pg_config = None
    if args.pg_config and os.path.exists(args.pg_config):
        with open(args.pg_config, 'r') as f:
            pg_config = json.load(f)
    
    # Load data to different destinations
    print("Loading data...")
    result = load_data(
        df,
        csv_path=args.output_path,
        load_to_gsheets=load_to_gsheets,
        load_to_pg=load_to_pg,
        credentials_path=args.gs_creds,
        pg_config=pg_config
    )
    
    # Print results summary
    print("\n=== Data Ingestion Summary ===")
    print(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Records processed: {len(df)}")
    print(f"CSV saved to: {result.get('csv_path')}")
    
    if load_to_gsheets:
        if result.get('gsheet_url'):
            print(f"Google Sheet: {result.get('gsheet_url')}")
        else:
            print("Google Sheet: Failed to upload")
    
    if load_to_pg:
        if result.get('postgres'):
            print("PostgreSQL: Successfully loaded")
        else:
            print("PostgreSQL: Failed to load")
    
    print("==============================")

if __name__ == "__main__":
    main()