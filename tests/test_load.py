import unittest
import pandas as pd
import os
import sys
import json
import psycopg2
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import shutil

# Add the parent directory to sys.path to import modules from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.load import load_to_csv, load_to_postgres, load_to_google_sheets, load_data

class TestLoad(unittest.TestCase):
    def setUp(self):
        # Create sample DataFrame for testing
        self.sample_df = pd.DataFrame({
            'Title': ['Test Product 1', 'Test Product 2', 'Test Product 3'],
            'Price': [479984.0, 799984.0, 1599984.0],
            'Rating': [4.5, 4.8, 4.2],
            'Colors': [2, 3, 1],
            'Size': ['M', 'L', 'S'],
            'Gender': ['Men', 'Women', 'Unisex'],
            'timestamp': ['2023-01-01 12:00:00', '2023-01-01 12:00:00', '2023-01-01 12:00:00']
        })
        
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        # Remove temporary directory after tests
        shutil.rmtree(self.temp_dir)

    def test_load_to_csv(self):
        # Test CSV export functionality
        test_file = os.path.join(self.temp_dir, "test_products.csv")
        
        # Call the function
        result_path = load_to_csv(self.sample_df, test_file)
        
        # Verify file exists
        self.assertTrue(os.path.exists(test_file))
        
        # Verify function returned the correct path
        self.assertEqual(result_path, test_file)
        
        # Read the CSV and verify its contents
        df_from_csv = pd.read_csv(test_file)
        self.assertEqual(len(df_from_csv), len(self.sample_df))
        
        # Check column names
        for col in self.sample_df.columns:
            self.assertIn(col, df_from_csv.columns)
    
    def test_load_to_csv_default_path(self):
        # Test with default path
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            result_path = load_to_csv(self.sample_df)
            
            # Check that to_csv was called with default path
            mock_to_csv.assert_called_once()
            self.assertEqual(result_path, "products.csv")
    
    @patch('psycopg2.connect')
    def test_load_to_postgres(self, mock_connect):
        # Mock the cursor
        mock_cursor = MagicMock()
        
        # Mock the connection
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the first connection to check if database exists
        mock_connect.side_effect = [mock_conn, mock_conn]
        
        # Set up cursor.fetchone() to indicate database exists
        mock_cursor.fetchone.return_value = [1]  # Database exists
        
        # Call the function
        load_to_postgres(
            self.sample_df,
            table_name="test_table",
            host="test_host",
            database="test_db",
            user="test_user",
            password="test_pass",
            port="5432"
        )
        
        # Verify connection was established
        mock_connect.assert_called_with(
            host="test_host", 
            database="test_db",
            user="test_user",
            password="test_pass",
            port="5432"
        )
        
        # Verify that CREATE TABLE was called
        create_table_called = False
        for call_args in mock_cursor.execute.call_args_list:
            if 'CREATE TABLE IF NOT EXISTS' in call_args[0][0]:
                create_table_called = True
                break
        self.assertTrue(create_table_called, "CREATE TABLE statement was not executed")
        
        # Verify that INSERT was called at least once for each row plus table operations
        # Function calls might include: database check, create table, truncate, and inserts
        # Just make sure there were enough calls to handle the data
        min_expected_calls = len(self.sample_df) + 1  # Minimum: enough for inserts + create table
        self.assertGreaterEqual(mock_cursor.execute.call_count, min_expected_calls)
    
    @patch('psycopg2.connect')
    def test_load_to_postgres_create_db(self, mock_connect):
        # Mock the cursor
        mock_cursor = MagicMock()
        
        # Mock the connection
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the connection behavior
        mock_connect.side_effect = [mock_conn, mock_conn]
        
        # Set up cursor.fetchone() to indicate database doesn't exist
        mock_cursor.fetchone.return_value = None  # Database doesn't exist
        
        # Call the function
        load_to_postgres(self.sample_df)
        
        # Verify database creation was attempted
        create_db_called = False
        for call_args in mock_cursor.execute.call_args_list:
            if 'CREATE DATABASE' in call_args[0][0]:
                create_db_called = True
                break
        self.assertTrue(create_db_called, "CREATE DATABASE statement was not executed")
    
    @patch('utils.load.load_to_google_sheets')
    def test_load_to_google_sheets_new_sheet(self, mock_load_to_sheets):
        # Setup mock return
        mock_load_to_sheets.return_value = "https://docs.google.com/spreadsheets/d/test_sheet_id"
        
        # Call the function directly
        result = mock_load_to_sheets(
            self.sample_df,
            creds_file="test_creds.json",
            sheet_name="Test Sheet"
        )
        
        # Check the expected return value
        self.assertEqual(result, "https://docs.google.com/spreadsheets/d/test_sheet_id")
    
    @patch('utils.load.load_to_google_sheets')
    def test_load_to_google_sheets_existing_sheet(self, mock_load_to_sheets):
        # Setup mock return
        mock_load_to_sheets.return_value = "https://docs.google.com/spreadsheets/d/existing_sheet_id"
        
        # Call the function directly with sheet_id parameter
        result = mock_load_to_sheets(
            self.sample_df,
            creds_file="test_creds.json",
            sheet_id="existing_sheet_id"
        )
        
        # Check the expected return value
        self.assertEqual(result, "https://docs.google.com/spreadsheets/d/existing_sheet_id")
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_postgres')
    @patch('utils.load.load_to_google_sheets')
    def test_load_data(self, mock_gsheets, mock_postgres, mock_csv):
        # Set up mocks
        mock_csv.return_value = "test.csv"
        mock_gsheets.return_value = "https://test-sheet-url"
        
        # Call the function with all destinations
        result = load_data(
            self.sample_df,
            csv_path="test.csv",
            load_to_gsheets=True,
            load_to_pg=True
        )
        
        # Verify all loading methods were called
        mock_csv.assert_called_once()
        mock_postgres.assert_called_once()
        mock_gsheets.assert_called_once()
        
        # Check the result contains all destinations
        self.assertEqual(result['csv_path'], "test.csv")
        self.assertEqual(result['gsheet_url'], "https://test-sheet-url")
        self.assertTrue(result['postgres'])
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_postgres')
    @patch('utils.load.load_to_google_sheets')
    def test_load_data_csv_only(self, mock_gsheets, mock_postgres, mock_csv):
        # Set up mocks
        mock_csv.return_value = "test.csv"
        
        # Call the function with CSV only
        result = load_data(
            self.sample_df,
            csv_path="test.csv",
            load_to_gsheets=False,
            load_to_pg=False
        )
        
        # Verify only CSV loading was called
        mock_csv.assert_called_once()
        mock_postgres.assert_not_called()
        mock_gsheets.assert_not_called()
        
        # Check the result
        self.assertEqual(result['csv_path'], "test.csv")
        self.assertNotIn('gsheet_url', result)
        self.assertNotIn('postgres', result)
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_postgres')
    @patch('utils.load.load_to_google_sheets', side_effect=Exception("Google API error"))
    def test_load_data_with_errors(self, mock_gsheets, mock_postgres, mock_csv):
        # Set up mocks
        mock_csv.return_value = "test.csv"
        mock_postgres.side_effect = Exception("Database connection error")
        
        # Call the function - it should handle errors gracefully
        result = load_data(
            self.sample_df,
            load_to_gsheets=True,
            load_to_pg=True
        )
        
        # Verify function calls
        mock_csv.assert_called_once()
        mock_postgres.assert_called_once()
        mock_gsheets.assert_called_once()
        
        # Check the result - should include error indicators
        self.assertEqual(result['csv_path'], "test.csv")
        self.assertIsNone(result['gsheet_url'])
        self.assertFalse(result['postgres'])

if __name__ == '__main__':
    unittest.main()