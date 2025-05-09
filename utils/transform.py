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
        # Skip items with invalid titles or known invalid patterns
        if "Unknown Product" in item["Title"]:
            continue
        
        # Clean price (convert to Rupiah)
        price = item["Price"]
        if price == "Price Unavailable" or price is None:
            continue
        
        try:
            # Extract only the number from price text (assuming format like "$49.99")
            price_value = re.search(r'[\d.]+', price)
            if price_value:
                # Convert to Rupiah (exchange rate: 16000)
                price_idr = float(price_value.group()) * 16000
            else:
                continue
        except:
            continue
        
        # Clean rating
        rating = item["Rating"]
        if rating == "Invalid Rating" or rating == "Not Rated" or "Invalid Rating" in rating:
            continue
        
        try:
            # Extract only the number from rating (assuming format like "4.8 / 5")
            rating_match = re.search(r'([\d.]+)', rating)
            rating_value = float(rating_match.group(1)) if rating_match else None
            if rating_value is None:
                continue
        except:
            continue
        
        # Clean colors (extract only the number)
        colors = item["Colors"]
        try:
            colors_match = re.search(r'(\d+)', colors)
            colors_value = int(colors_match.group(1)) if colors_match else 0
        except:
            colors_value = 0
        
        # Clean size (remove "Size: " prefix)
        size = item["Size"].replace("Size: ", "") if item["Size"] else ""
        
        # Clean gender (remove "Gender: " prefix)
        gender = item["Gender"].replace("Gender: ", "") if item["Gender"] else ""
        
        # Preserve the timestamp
        timestamp = item["timestamp"]
        
        cleaned_data.append({
            "Title": item["Title"],
            "Price": price_idr,
            "Rating": rating_value,
            "Colors": colors_value,
            "Size": size,
            "Gender": gender,
            "timestamp": timestamp  # Include timestamp in cleaned data
        })
    
    # Convert to DataFrame
    df = pd.DataFrame(cleaned_data)
    
    # Ensure we have the target number of rows or less
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
    
    # Add any additional transformations here if needed
    
    return df

if __name__ == "__main__":
    # For testing purposes
    from extract import extract_data
    
    raw_data = extract_data()
    df = transform_data(raw_data)
    print(df.info())