import os
import pandas as pd
import datetime
import json
import psycopg2
from google.oauth2 import service_account
from googleapiclient.discovery import build

def load_to_csv(df: pd.DataFrame, output_path: str = None) -> str:
    """
    Save the DataFrame to a CSV file.
    
    Args:
        df: DataFrame to save
        output_path: Path where the CSV should be saved
        
    Returns:
        The path where the file was saved
    """
    if output_path is None:
        output_path = "products.csv"
    
    df.to_csv(output_path, index=False)
    print(f"Data successfully saved to CSV: {output_path}")
    
    return output_path

def load_to_postgres(df: pd.DataFrame, 
                    table_name: str = "fashion_data",
                    host: str = "localhost", 
                    database: str = "fashion_db",
                    user: str = "postgres",
                    password: str = "postgres",
                    port: str = "5432") -> None:
    """
    Save the DataFrame to PostgreSQL.
    
    Args:
        df: DataFrame to save
        table_name: Name of the table to save data to
        host: PostgreSQL host
        database: PostgreSQL database name
        user: PostgreSQL username
        password: PostgreSQL password
        port: PostgreSQL port
    """
    try:
        print(f"Connecting to PostgreSQL at {host}:{port}, database: {database}")
        
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            database="postgres" 
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
        if cur.fetchone() is None:
            print(f"Database {database} does not exist. Creating it.")
            cur.execute(f"CREATE DATABASE \"{database}\"")
        
        cur.close()
        conn.close()
        
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Title" TEXT,
            "Price" FLOAT,
            "Rating" FLOAT,
            "Colors" INT,
            "Size" TEXT,
            "Gender" TEXT,
            "timestamp" TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        
        cur.execute(f"TRUNCATE TABLE {table_name};")
        
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} ("Title", "Price", "Rating", "Colors", "Size", "Gender", "timestamp")
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            timestamp = row['timestamp']
            if isinstance(timestamp, str):
                timestamp = pd.to_datetime(timestamp)
                
            cur.execute(insert_query, (
                row['Title'], 
                float(row['Price']), 
                float(row['Rating']), 
                int(row['Colors']), 
                row['Size'],
                row['Gender'],
                timestamp
            ))
        
        # Close the connection
        cur.close()
        conn.close()
        
        print(f"Data successfully loaded to PostgreSQL table: {table_name}")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_to_google_sheets(df: pd.DataFrame, 
                         creds_file: str = 'credentials.json',
                         sheet_name: str = "Fashion Data",
                         sheet_id: str = None) -> str:
    """
    Save the DataFrame to Google Sheets using Google API Python Client.
    Either updates an existing sheet or creates a new one.
    
    Args:
        df: DataFrame to save
        creds_file: Path to the Google API credentials JSON file
        sheet_name: Name of the Google Sheet
        sheet_id: ID of existing sheet to update (if None, creates new)
        
    Returns:
        The ID of the Google Sheet
    """
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive']
                  
        credentials = service_account.Credentials.from_service_account_file(
            creds_file, scopes=SCOPES
        )
        
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        drive_service = build('drive', 'v3', credentials=credentials)
        
        if sheet_id is None:
            try:
                results = drive_service.files().list(
                    q=f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet'",
                    fields="files(id, name)"
                ).execute()
                items = results.get('files', [])
                
                if items:
                    sheet_id = items[0]['id']
                    print(f"Found existing sheet: {sheet_name} (ID: {sheet_id})")
            except Exception as e:
                print(f"Error searching for existing sheet: {str(e)}")
        
        if not sheet_id:
            spreadsheet = {
                'properties': {
                    'title': sheet_name
                }
            }
            
            spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet).execute()
            sheet_id = spreadsheet.get('spreadsheetId')
            
            drive_service.permissions().create(
                fileId=sheet_id,
                body={
                    'type': 'anyone',
                    'role': 'writer'
                }
            ).execute()
            
            print(f"Created new sheet: {sheet_name} (ID: {sheet_id})")
        
        # Convert DataFrame to values for sheet
        values = [df.columns.tolist()]  
        values.extend(df.values.tolist()) 
        
        # First, clear the existing content
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range='Sheet1'
        ).execute()
        
        body = {
            'values': values
        }
        
        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='Sheet1!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        print(f"Data successfully loaded to Google Sheets: {sheet_url}")
        
        return sheet_url
    except Exception as e:
        print(f"Error loading data to Google Sheets: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_data(df: pd.DataFrame, 
              csv_path: str = None, 
              load_to_gsheets: bool = True, 
              load_to_pg: bool = True,
              credentials_path: str = 'google-sheets-api.json',
              pg_config: dict = None,
              sheet_id: str = None) -> dict:
    """
    Main function to load data to different destinations.
    
    Args:
        df: DataFrame to save
        csv_path: Path where the CSV should be saved (optional)
        load_to_gsheets: Whether to load to Google Sheets
        load_to_pg: Whether to load to PostgreSQL
        credentials_path: Path to Google API credentials
        pg_config: PostgreSQL configuration dictionary
        sheet_id: ID of existing sheet to update (if None, searches or creates new)
        
    Returns:
        Dictionary with paths/URLs to the saved data
    """
    result = {}
    
    csv_path = csv_path or "products.csv"
    csv_output_path = load_to_csv(df, csv_path)
    result['csv_path'] = csv_output_path
    
    if load_to_gsheets:
        try:
            gsheet_url = load_to_google_sheets(df, credentials_path, sheet_name="Fashion Data", sheet_id=sheet_id)
            result['gsheet_url'] = gsheet_url
        except Exception as e:
            print(f"Warning: Failed to save to Google Sheets. {str(e)}")
            result['gsheet_url'] = None
    
    if load_to_pg:
        try:
            if pg_config is None:
                pg_config = {
                    'host': 'localhost',
                    'database': 'fashion_db',
                    'user': 'postgres',
                    'password': 'postgres',
                    'port': '5432',
                    'table_name': 'fashion_data'
                }
            
            load_to_postgres(
                df, 
                table_name=pg_config.get('table_name', 'fashion_data'),
                host=pg_config.get('host', 'localhost'),
                database=pg_config.get('database', 'fashion_db'),
                user=pg_config.get('user', 'postgres'),
                password=pg_config.get('password', 'postgres'),
                port=pg_config.get('port', '5432')
            )
            result['postgres'] = True
        except Exception as e:
            print(f"Warning: Failed to save to PostgreSQL. {str(e)}")
            result['postgres'] = False
    
    return result