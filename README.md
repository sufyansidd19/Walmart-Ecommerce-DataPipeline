<p align="center">
  <img src="walmart.avif" alt="Walmart E-commerce Pipeline" width="700"/>
</p>


# 🛒 Walmart E-Commerce Data Pipeline

## 📖 Project Description

Walmart is the largest retail chain in the U.S. In 2022, its e-commerce revenue reached **$80 billion**, accounting for **13% of total sales**. To maintain efficiency and meet customer demands, Walmart must analyze sales patterns—especially around **public holidays** like Super Bowl, Labor Day, Thanksgiving, and Christmas.

This project creates a data pipeline to:

- Analyze supply and demand around holidays.
- Merge sales data with external economic data.
- Filter, clean, and aggregate the data.
- Export it into clean `.csv` files for insights.

---

## 🎯 Objective

- ✅ Build a full ETL pipeline using Python and Pandas.
- ✅ Clean and transform merged datasets.
- ✅ Analyze average monthly sales.
- ✅ Export cleaned and aggregated results to `.csv`.
- ✅ Validate successful file creation.

---

## 🗃️ Data Sources

### 📦 `grocery_sales` (PostgreSQL Table)

| Column        | Description              |
|---------------|--------------------------|
| index         | Unique row ID            |
| Store_ID      | Store number             |
| Date          | Week of sales            |
| Weekly_Sales  | Sales for the week/store |

### 📂 `extra_data.parquet` (Parquet File)

| Column         | Description                         |
|----------------|-------------------------------------|
| IsHoliday      | 1 if holiday week, else 0           |
| Temperature    | Region temperature                  |
| Fuel_Price     | Fuel cost                           |
| CPI            | Consumer Price Index                |
| Unemployment   | Unemployment rate                   |
| MarkDown1–4    | Promotional discounts               |
| Dept           | Store department number             |
| Size           | Store size                          |
| Type           | Store type                          |

---

## 🧰 Technologies Used

- Python 3.x
- Pandas
- PostgreSQL (Data source)
- Parquet file I/O
- CSV output

---

## 🔁 Full ETL Pipeline Code (Single Execution Block)

```python
import pandas as pd
import os

# Step 1: Extract
def extract(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on="index")
    return merged_df

# Step 2: Transform
def transform(raw_data):
    raw_data = raw_data.fillna(0)
    raw_data["Date"] = pd.to_datetime(raw_data["Date"], errors="coerce")
    raw_data = raw_data.dropna(subset=["Date"])
    raw_data["Month"] = raw_data["Date"].dt.month
    raw_data = raw_data[raw_data["Weekly_Sales"] > 10000]

    raw_data = raw_data[[
        "Store_ID", "Month", "Dept", "IsHoliday",
        "Weekly_Sales", "CPI", "Unemployment"
    ]]
    return raw_data

# Step 3: Aggregate
def avg_weekly_sales_per_month(clean_data):
    holiday_sales = (
        clean_data[["Month", "Weekly_Sales"]]
        .groupby("Month")
        .agg("mean")
        .reset_index()
        .round(2)
        .rename(columns={"Weekly_Sales": "Avg_Sales"})
    )
    return holiday_sales

# Step 4: Load
def load(clean_df, clean_path, agg_df, agg_path):
    clean_df.to_csv(clean_path, index=False)
    agg_df.to_csv(agg_path, index=False)

# Step 5: Validate
def validation(file_path):
    # Write your code here
    file_exists = os.path.exists(file_path)
    if not file_exists:
        raise Exception(f"There is no file at the path {file_path}")

# Run the pipeline
merged_df = extract(grocery_sales, "extra_data.parquet")
clean_data = transform(merged_df)
agg_data = avg_weekly_sales_per_month(clean_data)
load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")
validation("clean_data.csv")
validation("agg_data.csv")
