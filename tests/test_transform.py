import unittest
import pandas as pd
import sys
import os
import numpy as np
from unittest.mock import patch, MagicMock

# Add the parent directory to sys.path to import modules from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.transform import clean_data, transform_data

class TestTransform(unittest.TestCase):
    def setUp(self):
        # Set up sample raw data for testing
        self.raw_data = [
            {
                "Title": "Stylish T-Shirt",
                "Price": "$29.99",
                "Rating": "4.5 / 5",
                "Colors": "3 Colors",
                "Size": "Size: M",
                "Gender": "Gender: Unisex",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Designer Jeans",
                "Price": "$89.50",
                "Rating": "4.8 / 5",
                "Colors": "2 Colors",
                "Size": "Size: 32",
                "Gender": "Gender: Men",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Summer Dress",
                "Price": "$45.75",
                "Rating": "4.2 / 5",
                "Colors": "5 Colors",
                "Size": "Size: S",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        # Some invalid data samples to test edge cases
        self.invalid_data = [
            {
                "Title": "Unknown Product",  # Should be skipped
                "Price": "$29.99",
                "Rating": "4.5 / 5",
                "Colors": "3 Colors",
                "Size": "Size: M",
                "Gender": "Gender: Unisex",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Invalid Price",
                "Price": "Price Unavailable",  # Should be skipped
                "Rating": "4.8 / 5",
                "Colors": "2 Colors",
                "Size": "Size: 32",
                "Gender": "Gender: Men",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Invalid Rating",
                "Price": "$45.75",
                "Rating": "Invalid Rating",  # Should be skipped
                "Colors": "5 Colors",
                "Size": "Size: S",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
    
    def test_clean_data_valid_items(self):
        # Test with valid data
        result_df = clean_data(self.raw_data)
        
        # Check if DataFrame has correct number of rows
        self.assertEqual(len(result_df), 3)
        
        # Check if all required columns exist
        expected_columns = ["Title", "Price", "Rating", "Colors", "Size", "Gender", "timestamp"]
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check price conversion to Rupiah (USD * 16000)
        self.assertAlmostEqual(result_df.iloc[0]["Price"], 29.99 * 16000, delta=1)
        self.assertAlmostEqual(result_df.iloc[1]["Price"], 89.50 * 16000, delta=1)
        
        # Check rating conversion to float
        self.assertAlmostEqual(result_df.iloc[0]["Rating"], 4.5)
        self.assertAlmostEqual(result_df.iloc[1]["Rating"], 4.8)
        
        # Check colors conversion to int
        self.assertEqual(result_df.iloc[0]["Colors"], 3)
        self.assertEqual(result_df.iloc[1]["Colors"], 2)
        self.assertEqual(result_df.iloc[2]["Colors"], 5)
        
        # Check size prefix removal
        self.assertEqual(result_df.iloc[0]["Size"], "M")
        self.assertEqual(result_df.iloc[1]["Size"], "32")
        
        # Check gender prefix removal
        self.assertEqual(result_df.iloc[0]["Gender"], "Unisex")
        self.assertEqual(result_df.iloc[1]["Gender"], "Men")
    
    def test_clean_data_invalid_items(self):
        # Test with invalid data
        result_df = clean_data(self.invalid_data)
        
        # All items should be filtered out
        self.assertEqual(len(result_df), 0)
    
    def test_clean_data_mixed_items(self):
        # Test with a mix of valid and invalid data
        mixed_data = self.raw_data + self.invalid_data
        result_df = clean_data(mixed_data)
        
        # Only valid items should remain
        self.assertEqual(len(result_df), 3)
        
        # Check that all items have valid titles (not "Unknown Product")
        self.assertTrue(all(title != "Unknown Product" for title in result_df["Title"]))
    
    def test_clean_data_edge_cases(self):
        # Test edge cases with unusual data formats
        edge_cases = [
            {
                "Title": "Edge Case 1",
                "Price": "$0",  # Zero price
                "Rating": "5 / 5",  # Perfect rating
                "Colors": "1 Color",  # Singular "Color" rather than "Colors"
                "Size": "Size: One Size",
                "Gender": "Gender: Kids",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Edge Case 2",
                "Price": "$1,299.99",  # Price with comma
                "Rating": "0 / 5",  # Zero rating
                "Colors": "No colors available",  # No number
                "Size": "Size:",  # Empty size
                "Gender": "Gender: ",  # Empty gender
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        result_df = clean_data(edge_cases)
        
        # First edge case should be processed correctly
        self.assertEqual(result_df.iloc[0]["Price"], 0)
        self.assertEqual(result_df.iloc[0]["Rating"], 5.0)
        self.assertEqual(result_df.iloc[0]["Colors"], 1)
        
        # The second case might be filtered out or have default values
        if len(result_df) > 1:
            self.assertEqual(result_df.iloc[1]["Colors"], 0)
    
    def test_transform_data_row_limit(self):
        # Create a large dataset that exceeds 1000 rows
        large_data = [
            {
                "Title": f"Product {i}",
                "Price": f"${i}.99",
                "Rating": "4.5 / 5",
                "Colors": "3 Colors",
                "Size": "Size: M",
                "Gender": "Gender: Unisex",
                "timestamp": "2023-01-01 12:00:00"
            }
            for i in range(1, 1500)  # 1500 items
        ]
        
        # The transform function should limit to 1000 rows
        result_df = transform_data(large_data)
        
        # Check length is limited to 1000
        self.assertEqual(len(result_df), 1000)
    
    def test_transform_data_integration(self):
        # Test the main transform_data function
        # This is an integration test that ensures the function calls clean_data correctly
        
        with patch('utils.transform.clean_data') as mock_clean_data:
            # Setup mock return value
            mock_df = pd.DataFrame({
                "Title": ["Test Product"],
                "Price": [100000.0],
                "Rating": [4.5],
                "Colors": [3],
                "Size": ["M"],
                "Gender": ["Women"],
                "timestamp": ["2023-01-01 12:00:00"]
            })
            mock_clean_data.return_value = mock_df
            
            # Call the function
            result = transform_data(self.raw_data)
            
            # Verify clean_data was called with the raw data
            mock_clean_data.assert_called_once_with(self.raw_data)
            
            # Result should be the same as what clean_data returned
            pd.testing.assert_frame_equal(result, mock_df)

if __name__ == '__main__':
    unittest.main()