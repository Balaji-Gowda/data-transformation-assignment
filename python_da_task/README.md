Imported required Python Libraries

    import pandas as pd
    from datetime import datetime

Task1: Read raw CSV with all column datatypes as strings

    df = pd.read_csv("raw_sales.csv", dtype=str)  # read as string to handle quotes/blanks

Created User Defined Functions to do necessary data cleaning 

    # Task2: UDFs for cleaning
    def clean_int(value):
        try:
            val = int(float(value))
            return val if val >= 0 else 0
        except:
            return 0
    
    def clean_float(value):
        try:
            return float(value)
        except:
            return 0.0
    
    def clean_date(value):
        try:
            return pd.to_datetime(value, errors='coerce', dayfirst=False)
        except:
            return pd.NaT

Applied above UDFs to respective columns to clean the data
      
    df["order_id"] = df["order_id"].apply(clean_int)
    df["product_id"] = df["product_id"].apply(clean_int)
    df["quantity"] = df["quantity"].apply(clean_int)
    df["price_per_unit"] = df["price_per_unit"].apply(clean_float)
    df["order_date"] = df["order_date"].apply(clean_date)

Task3: Created another UDF for total_price calculation
    
    def compute_total_price(row):
        return row["quantity"] * row["price_per_unit"]

Task4: Derived new column by applying above UDF of Quantity, Price_per_unit columns

    df["total_price"] = df.apply(compute_total_price, axis=1)

Task5: Saved cleaned and transformed data to cleaned_sales.csv file

    df.to_csv("cleaned_sales.csv", index=False)
        
