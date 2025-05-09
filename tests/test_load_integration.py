import unittest
import sys
import os
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open

# Add the parent directory to sys.path to import modules from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.load import load_data

class TestLoadIntegration(unittest.TestCase):
    def setUp(self):
        # Create a sample DataFrame for testing
        self.test_data = pd.DataFrame({
            'Title': ['T-Shirt', 'Jeans', 'Shoes'],
            'Price': [250000.0, 800000.0, 650000.0],
            'Rating': [4.5, 4.8, 4.2],
            'Colors': [3, 2, 4],
            'Size': ['M', '32', '42'],
            'Gender': ['Unisex', 'Men', 'Women'],
            'timestamp': ['2023-01-01 12:00:00', '2023-01-01 12:00:00', '2023-01-01 12:00:00']
        })
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_postgres')
    @patch('utils.load.load_to_google_sheets')
    def test_load_data_with_pg_config_dict(self, mock_gsheets, mock_postgres, mock_csv):
        # Use a dictionary pg_config directly
        pg_config = {
            'host': 'test-host',
            'port': '5555',
            'database': 'test-db',
            'user': 'test-user',
            'password': 'test-pass',
            'table_name': 'test_table'
        }
        
        # Call load_data with the config dict
        load_data(
            self.test_data, 
            load_to_pg=True, 
            pg_config=pg_config
        )
        
        # Check that config was correctly applied to postgres function call
        mock_postgres.assert_called_once()
        call_kwargs = mock_postgres.call_args[1]
        self.assertEqual(call_kwargs['host'], 'test-host')
        self.assertEqual(call_kwargs['port'], '5555')
        self.assertEqual(call_kwargs['database'], 'test-db')
        self.assertEqual(call_kwargs['table_name'], 'test_table')
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_postgres', side_effect=Exception("Connection refused"))
    @patch('utils.load.load_to_google_sheets')
    def test_load_data_handle_postgres_error(self, mock_gsheets, mock_postgres, mock_csv):
        # Set up mock returns
        mock_csv.return_value = "output.csv"
        mock_gsheets.return_value = "https://docs.google.com/spreadsheets/id"
        
        # Call function - it should handle the postgres error gracefully
        result = load_data(
            self.test_data,
            load_to_gsheets=True,
            load_to_pg=True
        )
        
        # Check that all loading methods were called
        mock_csv.assert_called_once()
        mock_postgres.assert_called_once()
        mock_gsheets.assert_called_once()
        
        # Check that postgres error was handled
        self.assertEqual(result['postgres'], False)
        
        # Other destinations should still work
        self.assertEqual(result['csv_path'], "output.csv")
        self.assertEqual(result['gsheet_url'], "https://docs.google.com/spreadsheets/id")
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_postgres')
    def test_load_data_with_default_pg_config(self, mock_postgres, mock_csv):
        """Test using default pg_config."""
        # Set up the mock returns
        mock_csv.return_value = "test.csv"
        
        # Call without specifying pg_config
        result = load_data(
            self.test_data,
            load_to_pg=True  # Important: set this to True
        )
        
        # Should use default values for postgres
        mock_postgres.assert_called_once()
        call_kwargs = mock_postgres.call_args[1]
        self.assertEqual(call_kwargs['host'], 'localhost')
        self.assertEqual(call_kwargs['database'], 'fashion_db')
        
        # CSV should still be created
        mock_csv.assert_called_once()
        
        # Result should indicate postgres was successful
        self.assertTrue(result['postgres'])

if __name__ == '__main__':
    unittest.main()
