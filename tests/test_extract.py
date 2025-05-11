import unittest
from unittest.mock import patch, MagicMock
import datetime
import requests
import sys
import os
from bs4 import BeautifulSoup

# Add the parent directory to sys.path to import modules from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.extract import scrape_page, extract_data, BASE_URL

class TestExtract(unittest.TestCase):
    def setUp(self):
        # Create sample HTML content directly in the test
        self.sample_html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Fashion Studio</title>
        </head>
        <body>
            <div class="collection-card">
                <div class="product-details">
                    <h3 class="product-title">Classic White T-Shirt</h3>
                    <span class="price">$29.99</span>
                    <p style="font-weight: bold;">Rating: 4.5 / 5</p>
                    <p style="color: gray;">3 Colors</p>
                    <p style="font-style: italic;">Size: M</p>
                    <p style="text-decoration: underline;">Gender: Unisex</p>
                </div>
            </div>
            
            <div class="collection-card">
                <div class="product-details">
                    <h3 class="product-title">Designer Jeans</h3>
                    <span class="price">$89.99</span>
                    <p style="font-weight: bold;">Rating: 4.8 / 5</p>
                    <p style="color: gray;">2 Colors</p>
                    <p style="font-style: italic;">Size: 32</p>
                    <p style="text-decoration: underline;">Gender: Men</p>
                </div>
            </div>
            
            <div class="collection-card">
                <div class="product-details">
                    <h3 class="product-title">Summer Dress</h3>
                    <span class="price">$59.99</span>
                    <p style="font-weight: bold;">Rating: 4.2 / 5</p>
                    <p style="color: gray;">5 Colors</p>
                    <p style="font-style: italic;">Size: S</p>
                    <p style="text-decoration: underline;">Gender: Women</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Mock datetime to ensure consistent timestamps in tests
        self.patcher = patch('utils.extract.datetime')
        self.mock_datetime = self.patcher.start()
        self.mock_datetime.datetime.now.return_value = datetime.datetime(2023, 1, 1, 12, 0, 0)
    
    def tearDown(self):
        self.patcher.stop()
    
    @patch('utils.extract.requests.get')
    def test_scrape_page_success(self, mock_get):
        # Mock successful response
        mock_response = MagicMock()
        mock_response.text = self.sample_html
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call function
        result = scrape_page(BASE_URL)
        
        # Verify requests.get was called with correct params
        mock_get.assert_called_once()
        self.assertIn(BASE_URL, mock_get.call_args[0])
        
        # Check that at least some products were found
        self.assertTrue(len(result) > 0, "No products were extracted")
        
        # Check structure of a product
        product = result[0]
        required_keys = ["Title", "Price", "Rating", "Colors", "Size", "Gender", "timestamp"]
        for key in required_keys:
            self.assertIn(key, product, f"Product is missing '{key}' field")
        
        # Check that timestamp was added correctly
        self.assertEqual(product["timestamp"], "2023-01-01 12:00:00")
    
    def test_scrape_page_request_failure(self):
        # Use patch as context manager to simplify mocking
        with patch('utils.extract.requests.get') as mock_get:
            # Mock response
            mock_response = MagicMock()
            mock_response.text = self.sample_html
            mock_response.raise_for_status.return_value = None
            
            # Configure mock to fail twice then succeed
            mock_get.side_effect = [
                requests.RequestException("Connection error"),
                requests.RequestException("Timeout"),
                mock_response
            ]
            
            # Patch sleep to avoid waiting
            with patch('utils.extract.time.sleep'):
                # Call function
                result = scrape_page(BASE_URL)
                
                # Check retry count
                self.assertEqual(mock_get.call_count, 3)
    
    @patch('utils.extract.requests.get')
    def test_scrape_page_no_products(self, mock_get):
        # Mock response with no products
        mock_response = MagicMock()
        mock_response.text = "<html><body><div>No products here</div></body></html>"
        mock_get.return_value = mock_response
        
        # Call function
        result = scrape_page(BASE_URL)
        
        # Expect empty list
        self.assertEqual(result, [])
    
    @patch('utils.extract.requests.get')
    def test_scrape_page_parsing_error(self, mock_get):
        # Mock response with malformed product
        mock_response = MagicMock()
        mock_response.text = """
        <html>
            <body>
                <div class="collection-card">
                    <div class="product-details">
                        <!-- Missing title element -->
                        <span class="price">$49.99</span>
                    </div>
                </div>
            </body>
        </html>
        """
        mock_get.return_value = mock_response
        
        # Call function - should handle malformed HTML gracefully
        result = scrape_page(BASE_URL)
        
        # Should get a product with default/fallback values
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Title"], "Unknown Product")
    
    @patch('utils.extract.requests.get')
    def test_scrape_page_missing_elements(self, mock_get):
        # Menguji scraping pada halaman dengan elemen yang hilang
        mock_response = MagicMock()
        mock_response.text = """
            <div class="collection-card">
                <div class="product-details">
                    <h3 class="product-title">Incomplete Product</h3>
                </div>
            </div>
        """
        mock_get.return_value = mock_response
        
        # Call function
        result = scrape_page("https://missing-elements.com")
        
        # Check results for missing elements
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Title"], "Incomplete Product")
        self.assertEqual(result[0]["Price"], "Price Unavailable")  # Based on extract.py implementation
        
        # The following assertions might need adjustments based on your actual implementation
        # of how missing fields are handled in the extract.py file
        self.assertIn("Rating", result[0])
        self.assertIn("Colors", result[0])
        self.assertIn("Size", result[0])
        self.assertIn("Gender", result[0])
    
    @patch('utils.extract.scrape_page')
    def test_extract_data(self, mock_scrape_page):
        # Setup mock to return 3 products for first page, 2 for second, 0 for third
        mock_scrape_page.side_effect = [
            [{"Title": "Product 1", "Price": "$49.99", "Rating": "4.5 / 5", 
              "Colors": "3 Colors", "Size": "Size: M", "Gender": "Gender: Women", 
              "timestamp": "2023-01-01 12:00:00"}] * 3,
            [{"Title": "Product 4", "Price": "$59.99", "Rating": "4.0 / 5", 
              "Colors": "2 Colors", "Size": "Size: L", "Gender": "Gender: Men", 
              "timestamp": "2023-01-01 12:00:00"}] * 2,
            # Return empty for all remaining pages to stop the pagination early
            []  # No products on third page
        ]
        
        # Patch MAX_PAGES to a smaller number to avoid unnecessary iterations
        with patch('utils.extract.MAX_PAGES', 3):
            # Patch sleep to avoid waiting during tests
            with patch('utils.extract.time.sleep'):
                # Call extract_data
                result = extract_data()
                
                # Should have products from first two pages only
                self.assertEqual(len(result), 5)
                
                # Verify mock was called for each page until no products found
                self.assertGreaterEqual(mock_scrape_page.call_count, 3)
    
    @patch('utils.extract.scrape_page')
    def test_extract_data_with_target_limit(self, mock_scrape_page):
        # Setup to return many products
        mock_scrape_page.return_value = [
            {"Title": f"Product {i}", "Price": "$49.99", "Rating": "4.5 / 5",
             "Colors": "3 Colors", "Size": "Size: M", "Gender": "Gender: Women",
             "timestamp": "2023-01-01 12:00:00"}
            for i in range(500)  # Each page has 500 products
        ]
        
        # Patch TARGET_DATA to a smaller number to test limit
        with patch('utils.extract.TARGET_DATA', 1000):
            with patch('utils.extract.time.sleep'):
                result = extract_data()
                
                # Should stop after hitting TARGET_DATA
                self.assertLessEqual(len(result), 1000)
                # Should have called scrape_page 2 times (for 2 pages) to get 1000 products
                self.assertEqual(mock_scrape_page.call_count, 2)

    @patch('utils.extract.requests.get')
    def test_scrape_simple_html(self, mock_get):
        # Menguji scraping dengan HTML yang valid dalam format sederhana
        mock_response = MagicMock()
        mock_response.text = """
            <div class="collection-card">
                <div class="product-details">
                    <h3 class="product-title">Test Product</h3>
                    <span class="price">$10</span>
                    <p style="font-weight: bold;">Rating: 4.5 / 5</p>
                    <p style="color: gray;">3 Colors</p>
                    <p style="font-style: italic;">Size: M</p>
                    <p style="text-decoration: underline;">Gender: Unisex</p>
                </div>
            </div>
        """
        mock_get.return_value = mock_response
        
        # Call function
        result = scrape_page("https://test-url.com")
        self.assertEqual(len(result), 1)
        
        # Check extracted data
        self.assertEqual(result[0]["Title"], "Test Product")
        self.assertEqual(result[0]["Price"], "$10")
        self.assertEqual(result[0]["Rating"], "4.5 / 5")
        self.assertEqual(result[0]["Colors"], "3 Colors")
        self.assertEqual(result[0]["Size"], "Size: M")
        self.assertEqual(result[0]["Gender"], "Gender: Unisex")
        self.assertEqual(result[0]["timestamp"], "2023-01-01 12:00:00")
    
    @patch('utils.extract.requests.get')
    def test_scrape_page_timeout(self, mock_get):
        # Menguji scraping saat timeout terjadi
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")
        
        # Patch sleep to avoid waiting during tests
        with patch('utils.extract.time.sleep'):
            result = scrape_page("https://timeout-url.com")
            self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()