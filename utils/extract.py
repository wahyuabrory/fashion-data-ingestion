import time
import requests
from bs4 import BeautifulSoup
import datetime

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

BASE_URL = "https://fashion-studio.dicoding.dev/"
MAX_PAGES = 50
TARGET_DATA = 1000
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2

def scrape_page(url: str) -> list:
    """
    Scrape a single page and return a list of products.

    Parameters:
    url (str): URL of the page to scrape

    Returns:
    list: List of products with attributes like Title, Price, Rating, Colors, Size, Gender, and timestamp
    """
    products = []
    attempts = 0
    
    while attempts < RETRY_ATTEMPTS:
        try:
            print(f"Attempting to fetch {url} (attempt {attempts + 1}/{RETRY_ATTEMPTS})")
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            attempts += 1
            print(f"Error fetching {url}: {e}")
            if attempts < RETRY_ATTEMPTS:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Failed to fetch {url} after {RETRY_ATTEMPTS} attempts")
                return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Get current timestamp for all products on this page
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Look for collection cards (the main product card containers)
    collection_cards = soup.find_all("div", class_="collection-card")
    
    if not collection_cards:
        print(f"No collection cards found on {url}.")
    
    for card in collection_cards:
        try:
            # Find the product details div within the collection card
            product_details = card.find("div", class_="product-details")
            if not product_details:
                continue
            
            # Extract title
            title_element = product_details.find("h3", class_="product-title")
            title = title_element.text.strip() if title_element else "Unknown Product"
            
            # Extract price - may be in span.price or p.price
            price_element = product_details.find("span", class_="price")
            if not price_element:
                price_element = product_details.find("p", class_="price")
            price = price_element.text.strip() if price_element else "Price Unavailable"
            
            # Extract all paragraph elements with styling
            paragraphs = product_details.find_all("p", style=True)
            
            rating = "Invalid Rating"
            colors = "0 Colors"
            size = "Size: N/A"
            gender = "Gender: N/A"
            
            for p in paragraphs:
                text = p.text.strip()
                if "Rating:" in text:
                    rating = text.replace("Rating:", "").strip()
                elif "Colors" in text:
                    colors = text.strip()
                elif "Size:" in text:
                    size = text.strip()
                elif "Gender:" in text:
                    gender = text.strip()
            
            # Create product dictionary
            products.append({
                "Title": title,
                "Price": price,
                "Rating": rating,
                "Colors": colors,
                "Size": size,
                "Gender": gender,
                "timestamp": timestamp
            })
            
        except Exception as e:
            print(f"Error extracting product data: {e}")
    
    print(f"Successfully extracted {len(products)} products from {url}")
    return products

def extract_data() -> list:
    """
    Scrape all pages up to the maximum limit or target data amount.

    Returns:
    list: List of all products successfully scraped
    """
    all_products = []
    page_count = 0
    
    print(f"Starting extraction process. Target: {TARGET_DATA} products from up to {MAX_PAGES} pages")
    
    for page in range(1, MAX_PAGES + 1):
        try:
            page_count += 1
            
            # Construct page URL - website uses /page{number} format for pagination
            if page == 1:
                url = BASE_URL
            else:
                url = f"{BASE_URL}page{page}"
                
            print(f"Scraping page {page}/{MAX_PAGES}: {url}")
            
            # Scrape the page
            page_products = scrape_page(url)
            
            # Add products to the main list
            all_products.extend(page_products)
            
            print(f"Progress: {len(all_products)}/{TARGET_DATA} products collected")
            
            # Check if we have enough data
            if len(all_products) >= TARGET_DATA:
                print(f"Reached target of {TARGET_DATA} products. Stopping extraction.")
                break
                
            # If no products found on this page, we might have reached the end
            if len(page_products) == 0:
                print(f"No products found on page {page}. This might be the last page or an issue with the site.")
                # Try one more page before giving up
                if page > 1:
                    next_url = f"{BASE_URL}page{page+1}"
                    if len(scrape_page(next_url)) == 0:
                        print("Confirmed end of product listings. Stopping extraction.")
                        break
            
            # Add delay between requests to be considerate to the server
            time.sleep(1.5)
            
        except Exception as e:
            print(f"Error processing page {page}: {e}")
    
    print(f"Extraction complete. Total pages scraped: {page_count}")
    print(f"Total products extracted: {len(all_products)}")
    
    return all_products

if __name__ == "__main__":
    # For testing purposes
    raw_data = extract_data()
    print(f"Extracted {len(raw_data)} items")
