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
        if "Unknown Product" in item["Title"]:
            continue
        
        # Clean price (convert to Rupiah)
        price = item["Price"]
        if price == "Price Unavailable" or price is None:
            continue
        
        try:
            price_value = re.search(r'[\d.]+', price)
            if price_value:
                price_idr = float(price_value.group()) * 16000
            else:
                continue
        except:
            continue
        
        rating = item["Rating"]
        if rating == "Invalid Rating" or rating == "Not Rated" or "Invalid Rating" in rating:
            continue
        
        try:
            rating_match = re.search(r'([\d.]+)', rating)
            rating_value = float(rating_match.group(1)) if rating_match else None
            if rating_value is None:
                continue
        except:
            continue
        
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