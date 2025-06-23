import pandas as pd
import os

# Extraction Part

def extract(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on = "index")
    return merged_df

merged_df = extract(grocery_sales, "extra_data.parquet")

# Transformation Part
def transform(raw_data):
    raw_data=raw_data.fillna(0)
    # print(merged_df)
    raw_data["Date"] = pd.to_datetime(raw_data["Date"], format = "%Y-%m-%d",errors='coerce') 
    raw_data['Month'] = raw_data['Date'].dt.month
    raw_data = raw_data[raw_data['Weekly_Sales'] > 10000]
    raw_data = raw_data[["Store_ID", "Month", "Dept", "IsHoliday", "Weekly_Sales", "CPI", 
    "Unemployment"]]
    print(raw_data)
    return raw_data

# Calling the transform() function and pass the merged DataFrame
clean_data = transform(merged_df)

# Create the avg_weekly_sales_per_month function that takes in the cleaned data from the last step
def avg_weekly_sales_per_month(clean_data):
    holiday_sales=clean_data[["Month","Weekly_Sales"]]
    holiday_sales=(holiday_sales.groupby("Month").agg("mean").reset_index().round(2))
    return holiday_sales

# Calling the avg_weekly_sales_per_month() function and pass the cleaned DataFrame
agg_data = avg_weekly_sales_per_month(clean_data)

# Create the load() function that takes in the cleaned DataFrame and the aggregated one with the paths where they are going to be stored
def load(full_data, full_data_file_path, agg_data, agg_data_file_path):
    # Write your code here
    full_data.to_csv(full_data_file_path,index=False)
    agg_data.to_csv(agg_data_file_path,index=False)

# Calling the load() function and pass the cleaned and aggregated DataFrames with their paths  
load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")

# Creating the validation() function with one parameter: file_path - to check whether the previous function was correctly executed
def validation(file_path):
    # Write your code here
    file_exists = os.path.exists(file_path)
    if not file_exists:
        raise Exception(f"There is no file at the path {file_path}")

# Call the validation() function and pass first, the cleaned DataFrame path, and then the aggregated DataFrame path
validation("clean_data.csv")
validation("agg_data.csv")
