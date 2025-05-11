import re
import pandas as pd


def clean_data(raw_data: list) -> pd.DataFrame:
    """
    Clean the raw extracted data according to requirements.
    
    Args:
        raw_data: List of raw data items from extraction
        
    Returns:
        DataFrame with cleaned data according to specifications
    """
    cleaned_data = []
    
    for item in raw_data:
        # Check for required fields and handle missing keys
        if not item:
            continue
            
        # Check for missing title
        if "Title" not in item or not item["Title"]:
            continue
            
        # Skip unknown products
        if "Unknown Product" in item.get("Title", ""):
            continue
        
        # Clean price (convert to Rupiah)
        price = item.get("Price", "Price Unavailable")
        if price == "Price Unavailable" or price is None:
            continue
        
        try:
            price_value = re.search(r'[\d.,]+', price)
            if price_value:
                # Remove commas and convert to float
                price_str = price_value.group().replace(',', '')
                price_idr = float(price_str) * 16000
            else:
                continue
        except Exception:
            continue
        
        # Skip items without rating
        if "Rating" not in item or not item["Rating"]:
            continue
            
        rating = item["Rating"]
        if rating == "Invalid Rating" or rating == "Not Rated" or "Invalid Rating" in rating:
            continue
        
        try:
            rating_match = re.search(r'([\d.]+)', rating)
            rating_value = float(rating_match.group(1)) if rating_match else None
            if rating_value is None:
                continue
        except Exception:
            continue
        
        # Handle colors - more robust extraction
        colors = item.get("Colors", "0")
        try:
            colors_match = re.search(r'(\d+)', colors)
            colors_value = int(colors_match.group(1)) if colors_match else 0
        except Exception:
            colors_value = 0
        
        # Clean size (remove "Size: " prefix) - handle None values
        size = item.get("Size", "")
        size = size.replace("Size: ", "") if size else ""
        
        # Clean gender (remove "Gender: " prefix) - handle None values
        gender = item.get("Gender", "")
        gender = gender.replace("Gender: ", "") if gender else ""
        
        # Preserve the timestamp
        timestamp = item.get("timestamp", "")
        
        # Add the item to the cleaned data list if it passes all checks
        cleaned_data.append({
            "Title": item["Title"],
            "Price": price_idr,
            "Rating": rating_value,
            "Colors": colors_value,
            "Size": size,
            "Gender": gender,
            "timestamp": timestamp 
        })
    
    df = pd.DataFrame(cleaned_data)
    
    if len(df) > 1000:
        df = df.head(1000)
    
    print(f"Transformation complete. Total valid items: {len(df)}")
    
    return df

def transform_data(raw_data: list) -> pd.DataFrame:
    """
    Main function to transform the raw data.
    
    Args:
        raw_data: List of raw data items from extraction
        
    Returns:
        DataFrame with transformed data
    """
    df = clean_data(raw_data)
        
    return df

if __name__ == "__main__":
    from extract import extract_data
    
    raw_data = extract_data()
    df = transform_data(raw_data)
    print(df.info())