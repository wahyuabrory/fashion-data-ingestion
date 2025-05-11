import unittest
import sys
import os
import pandas as pd
import logging
from unittest.mock import patch, MagicMock

# Add the parent directory to sys.path to import modules from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.load import load_to_google_sheets

# Set up logging for tests that check log output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestGoogleSheets(unittest.TestCase):
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

    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.build')
    def test_load_to_google_sheets_success(self, mock_build, mock_credentials):
        # Menguji apakah data berhasil diupload ke Google Sheets
        # Mock the Google API services
        mock_sheets_service = MagicMock()
        mock_drive_service = MagicMock()
        
        # Setup the build function to return our mocked services
        mock_build.side_effect = [mock_sheets_service, mock_drive_service]
        
        # Mock the sheets API responses
        mock_sheets = mock_sheets_service.spreadsheets.return_value
        mock_create = mock_sheets.create.return_value
        mock_create.execute.return_value = {"spreadsheetId": "test_sheet_id"}
        
        # Mock the values API responses
        mock_values = mock_sheets.values.return_value
        mock_clear = mock_values.clear.return_value
        mock_clear.execute.return_value = {}
        mock_update = mock_values.update.return_value
        mock_update.execute.return_value = {"updatedRows": 4}  # Header + 3 data rows
        
        # Mock the drive API for permissions
        mock_files = mock_drive_service.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {"files": []}  # No existing sheet
        
        mock_permissions = mock_drive_service.permissions.return_value
        mock_create_permission = mock_permissions.create.return_value
        mock_create_permission.execute.return_value = {}
        
        # Call the function
        result = load_to_google_sheets(
            self.test_data,
            creds_file='google-sheets-api.json',
            sheet_name="Test Sheet"
        )
        
        # Verify the Google API calls
        # 1. Check that files.list was called to search for existing sheets
        mock_files.list.assert_called_once()
        self.assertIn("name='Test Sheet'", str(mock_files.list.call_args))
        
        # 2. Check that spreadsheets.create was called to create the sheet
        mock_sheets.create.assert_called_once()
        create_body = mock_sheets.create.call_args[1]['body']
        self.assertEqual(create_body['properties']['title'], "Test Sheet")
        
        # 3. Check that values.update was called with the right data
        mock_values.update.assert_called_once()
        update_body = mock_values.update.call_args[1]['body']
        
        # Verify the first row is the header
        self.assertEqual(update_body['values'][0], list(self.test_data.columns))
        
        # Verify the returned URL
        self.assertEqual(result, f"https://docs.google.com/spreadsheets/d/test_sheet_id")
    
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.build')
    def test_load_to_google_sheets_existing(self, mock_build, mock_credentials):
        # Menguji apakah reuse sheet yang sudah ada berfungsi dengan baik
        # Mock the Google API services
        mock_sheets_service = MagicMock()
        mock_drive_service = MagicMock()
        
        # Setup the build function to return our mocked services
        mock_build.side_effect = [mock_sheets_service, mock_drive_service]
        
        # Mock the drive API to return an existing sheet
        mock_files = mock_drive_service.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {
            "files": [{"id": "existing_id", "name": "Test Sheet"}]
        }
        
        # Mock the values API responses
        mock_values = mock_sheets_service.spreadsheets.return_value.values.return_value
        mock_clear = mock_values.clear.return_value
        mock_clear.execute.return_value = {}
        mock_update = mock_values.update.return_value
        mock_update.execute.return_value = {"updatedRows": 4}
        
        # Call the function
        result = load_to_google_sheets(
            self.test_data,
            creds_file='google-sheets-api.json',
            sheet_name="Test Sheet"
        )
        
        # Verify that create was NOT called (reusing existing sheet)
        self.assertEqual(mock_sheets_service.spreadsheets.return_value.create.call_count, 0)
        
        # Verify returned URL uses the existing sheet ID
        self.assertEqual(result, "https://docs.google.com/spreadsheets/d/existing_id")
    
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.build')
    def test_load_to_google_sheets_api_error(self, mock_build, mock_credentials):
        # Menguji apakah function menangani error API dengan baik
        # Mock that the create spreadsheet call fails
        mock_sheets_service = MagicMock()
        mock_drive_service = MagicMock()
        
        # Setup the build function to return our mocked services
        mock_build.side_effect = [mock_sheets_service, mock_drive_service]
        
        # First allow drive search to succeed but return empty results
        mock_files = mock_drive_service.files.return_value
        mock_list = mock_files.list.return_value
        mock_list.execute.return_value = {"files": []}
        
        # Then make spreadsheet creation fail
        mock_sheets = mock_sheets_service.spreadsheets.return_value
        mock_create = mock_sheets.create.return_value
        mock_create.execute.side_effect = Exception("Spreadsheet creation failed")
        
        # Test that the function reports the error properly
        with self.assertRaises(Exception) as context:
            load_to_google_sheets(
                self.test_data,
                creds_file='google-sheets-api.json',
                sheet_name="Test Sheet"
            )
        
        self.assertIn("Spreadsheet creation failed", str(context.exception))
    
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.build')
    def test_load_to_google_sheets_empty_dataframe(self, mock_build, mock_credentials):
        # Menguji apakah function menangani dataframe kosong dengan baik
        mock_sheets_service = MagicMock()
        mock_drive_service = MagicMock()
        
        # Setup the build function to return our mocked services
        mock_build.side_effect = [mock_sheets_service, mock_drive_service]
        
        # Create empty DataFrame
        empty_df = pd.DataFrame()
        
        # Call the function with empty DataFrame
        # Should handle this gracefully without error
        result = load_to_google_sheets(
            empty_df,
            creds_file='google-sheets-api.json',
            sheet_name="Empty Test Sheet"
        )
        
        # Should still return a URL
        self.assertTrue(result.startswith("https://docs.google.com/spreadsheets/d/"))

if __name__ == '__main__':
    unittest.main()
