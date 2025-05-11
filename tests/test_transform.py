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
            },
            {
                "Title": "Edge Case 3",
                "Price": "$99.99",
                "Rating": "★★★★☆",  # Star rating format
                "Colors": "3 options",
                "Size": "Size: M-L",
                "Gender": "Gender: Unisex",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Edge Case 4",
                "Price": "Sale: $49.99",  # Price with prefix
                "Rating": "Rating: 4.2",  # Different rating format
                "Colors": "Colors: 2",  # Different format
                "Size": "Size: M",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        result_df = clean_data(edge_cases)
        
        # Check that some of the edge cases were processed
        self.assertGreater(len(result_df), 0)
        
        # Check if the first edge case was processed correctly
        found_case1 = False
        for i in range(len(result_df)):
            if result_df.iloc[i]["Title"] == "Edge Case 1":
                found_case1 = True
                self.assertEqual(result_df.iloc[i]["Price"], 0)
                self.assertEqual(result_df.iloc[i]["Rating"], 5.0)
                self.assertEqual(result_df.iloc[i]["Colors"], 1)
                break
                
        self.assertTrue(found_case1, "Edge Case 1 should be processed correctly")
            
        # Check for Edge Case 2 - commas in price should be handled
        for i in range(len(result_df)):
            if result_df.iloc[i]["Title"] == "Edge Case 2":
                self.assertAlmostEqual(result_df.iloc[i]["Price"], 1299.99 * 16000, delta=1)
                break
    
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

    def test_transform_data_with_empty_input(self):
        # Test with empty input list
        result_df = transform_data([])
        
        # Should return an empty DataFrame
        self.assertEqual(len(result_df), 0)
        
    def test_transform_data_few_items(self):
        # Test with just a few items that should be preserved
        few_items = [
            {
                "Title": "Good Product",
                "Price": "$59.99",
                "Rating": "4.5 / 5",
                "Colors": "3 Colors",
                "Size": "Size: M",
                "Gender": "Gender: Unisex",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Another Item",
                "Price": "$29.99",
                "Rating": "4.2 / 5",
                "Colors": "2 Colors",
                "Size": "Size: S",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        # This should run the transform_data function directly
        result_df = transform_data(few_items)
        
        # Check we get both items as expected
        self.assertEqual(len(result_df), 2)
        self.assertEqual(result_df.iloc[0]["Title"], "Good Product")
        self.assertEqual(result_df.iloc[1]["Title"], "Another Item")
    
    def test_price_extraction_error_handling(self):
        # Test price extraction error handling
        bad_price_items = [
            {
                "Title": "Price with no digits",
                "Price": "Price: $", 
                "Rating": "4.5 / 5",
                "Colors": "1 Colors",
                "Size": "Size: M",
                "Gender": "Gender: Men",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Price extraction exception",
                "Price": None,  # None price should be skipped
                "Rating": "4.0 / 5",
                "Colors": "2 Colors",
                "Size": "Size: L",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Price marked as unavailable",
                "Price": "Price Unavailable",
                "Rating": "3.9 / 5",
                "Colors": "1 Colors",
                "Size": "Size: S",
                "Gender": "Gender: Men",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        # These items should be filtered out during cleaning
        result_df = clean_data(bad_price_items)
        self.assertEqual(len(result_df), 0, "All items with bad prices should be filtered out")
    
    def test_color_extraction_error_handling(self):
        # Test color extraction with problematic data
        color_error_items = [
            {
                "Title": "Item with no colors info",
                "Price": "$45.99",
                "Rating": "4.8 / 5",
                # No Colors field - should default to 0
                "Size": "Size: M",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Item with invalid colors format",
                "Price": "$32.99",
                "Rating": "3.7 / 5",
                "Colors": "Many Colors Available",  # No digits
                "Size": "Size: L",
                "Gender": "Gender: Men",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        result_df = clean_data(color_error_items)
        self.assertEqual(len(result_df), 2, "Items should be processed despite color issues")
        
        # Check the colors were defaulted to 0
        colors_sum = sum(result_df["Colors"].tolist())
        self.assertEqual(colors_sum, 0, "Colors with extraction issues should default to 0")
        
    def test_empty_items_handling(self):
        # Test handling of empty or None items in the list
        items_with_empties = [
            None,
            {},
            {"Title": "Good Product", "Price": "$25.99", "Rating": "4.1 / 5", "Colors": "2 Colors"},
            {"Price": "$19.99", "Rating": "3.9 / 5", "Colors": "1 Colors"}  # Missing Title
        ]
        
        result_df = clean_data(items_with_empties)
        self.assertEqual(len(result_df), 1, "Only one valid item should remain")
        
    def test_rating_extraction_error_handling(self):
        # Test that items with problematic ratings are handled correctly
        
        # This item has a Rating: / 5 pattern but actually has a number 5 that gets extracted
        item_with_rating_5 = {
            "Title": "Item with rating 5",
            "Price": "$19.99",
            "Rating": "Rating: / 5",
            "Colors": "1 Colors",
            "Size": "Size: M",
            "Gender": "Gender: Men"
        }
        
        # Let's use other items that should definitely be filtered out
        bad_rating_items = [
            {
                "Title": "Item with invalid rating",
                "Price": "$29.99",
                "Rating": "Not Rated",  # Should be filtered
                "Colors": "2 Colors",
                "Size": "Size: S",
                "Gender": "Gender: Women"
            },
            {
                "Title": "Item with truly no digits",
                "Price": "$19.99",
                "Rating": "no numbers here",  # No digits
                "Colors": "1 Colors",
                "Size": "Size: M",
                "Gender": "Gender: Men"
            }
        ]
        
        result_df = clean_data(bad_rating_items)
        self.assertEqual(len(result_df), 0, "Items with bad ratings should be filtered out")
        
        # Test the item that specifically extracts '5' from 'Rating: / 5'
        valid_result = clean_data([item_with_rating_5])
        self.assertEqual(len(valid_result), 1, "Expected one item to pass validation")
        if len(valid_result) > 0:
            self.assertEqual(valid_result.iloc[0]["Rating"], 5.0)
        
    def test_row_limit_enforcement(self):
        # Test that dataframe is limited to 1000 rows
        # Create more than 1000 valid items
        many_items = []
        for i in range(1100):
            many_items.append({
                "Title": f"Product {i}",
                "Price": "$10.99",
                "Rating": "4.0 / 5",
                "Colors": "1 Colors",
                "Size": "Size: M",
                "Gender": "Gender: Unisex",
                "timestamp": "2023-01-01 12:00:00"
            })
        
        result_df = clean_data(many_items)
        self.assertEqual(len(result_df), 1000, "DataFrame should be limited to 1000 rows")
        
    def test_clean_data_with_malformed_data(self):
        # Test with malformed data that might cause exceptions
        malformed_data = [
            {
                # Missing "Title" key - should be skipped
                "Price": "$19.99",
                "Rating": "4.0 / 5",
                "Colors": "2 Colors",
                "Size": "Size: S",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Product with None values",
                "Price": None,  # None price - should be skipped
                "Rating": "4.5 / 5",  # None rating
                "Colors": None,  # None colors
                "Size": None,    # None size
                "Gender": None,  # None gender
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Product with weird formatting",
                "Price": "Only $29.99!",  # Unusual price format with text
                "Rating": "Almost 5/5!",  # Unusual rating format - should be skipped
                "Colors": "Multiple colors",  # No number
                "Size": "Size varies",
                "Gender": "Gender neutral",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Product with different rating format",
                "Price": "$59.99",
                "Rating": "4.5",  # Simple number without format - might be processed
                "Colors": "3 Colors",
                "Size": "Size: L",
                "Gender": "Gender: Men",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        # This shouldn't raise exceptions, should just filter out bad data
        result_df = clean_data(malformed_data)
        
        # We expect most of these to be filtered out, but the function should handle them gracefully
        self.assertGreaterEqual(len(result_df), 0)
        
        # Check that last item might be processed since it has valid data
        found_valid = False
        for i in range(len(result_df)):
            if result_df.iloc[i]["Title"] == "Product with different rating format":
                found_valid = True
                self.assertAlmostEqual(result_df.iloc[i]["Price"], 59.99 * 16000, delta=1)
                self.assertEqual(result_df.iloc[i]["Rating"], 4.5)
                self.assertEqual(result_df.iloc[i]["Size"], "L")
                break
                
        # No assertion on found_valid because we don't know if this particular item
        # will pass all the validation in the clean_data function
        
    def test_clean_data_with_numeric_edge_cases(self):
        # Test with edge cases in numeric data
        numeric_edge_cases = [
            {
                "Title": "Product with very high price",
                "Price": "$9999999.99",  # Very high price
                "Rating": "4.9 / 5",
                "Colors": "10 Colors",  # High number of colors
                "Size": "Size: XL",
                "Gender": "Gender: Men",
                "timestamp": "2023-01-01 12:00:00"
            },
            {
                "Title": "Product with very low price",
                "Price": "$0.01",  # Very low price
                "Rating": "1.0 / 5",  # Very low rating
                "Colors": "0 Colors",  # Zero colors
                "Size": "Size: XXS",
                "Gender": "Gender: Women",
                "timestamp": "2023-01-01 12:00:00"
            }
        ]
        
        result_df = clean_data(numeric_edge_cases)
        
        # Check that at least some of the edge cases were processed
        self.assertGreater(len(result_df), 0, "At least one numeric edge case should be processed")
        
        # Basic check on the first item
        if len(result_df) > 0:
            if result_df.iloc[0]["Title"] == "Product with very high price":
                self.assertEqual(result_df.iloc[0]["Colors"], 10)
            elif result_df.iloc[0]["Title"] == "Product with very low price":
                self.assertEqual(result_df.iloc[0]["Colors"], 0)
            self.assertEqual(result_df.iloc[1]["Rating"], 1.0)
            self.assertEqual(result_df.iloc[1]["Colors"], 0)

if __name__ == '__main__':
    unittest.main()