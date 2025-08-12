# Data Warehouse & Power BI Sales Analysis

## 📌 Project Overview
This project demonstrates the process of building a **Data Warehouse** from operational data (SQL + CSV), performing **ETL** to transform and load the data into a **Star Schema**, and creating an **interactive Power BI dashboard** for sales analysis with drill-down and roll-up functionality.

---

## 1️⃣ Data Sources
- **stores-and-products.sql** – Contains store, location, and product hierarchy data.
- **sales.csv** – Contains sales transactions (date, shop, article, quantity, turnover) for the last 5 months.

---

## 2️⃣ ETL Process
The ETL process was implemented in Python using:
- **pandas** for data cleaning and transformation.
- **psycopg2** for PostgreSQL connection and data loading.

### Steps:
1. **Extract**
   - Load `stores-and-products.sql` into PostgreSQL (creates OLTP tables like `country`, `region`, `city`, `shop`, `article`, etc.).
   - Read `sales.csv` and clean date/number formats, remove invalid rows.

2. **Transform**
   - Flatten location hierarchy: `shop → city → region → country` into `dim_shop`.
   - Flatten product hierarchy: `article → product_group → product_family → product_category` into `dim_product`.
   - Generate `dim_date` table with year, month, quarter.
   - Map natural keys (shop_id, article_id, date) to surrogate keys.

3. **Load**
   - Create and populate **Star Schema**:
     - `dim_date`, `dim_shop`, `dim_product`
     - `fact_sales_star` (grain: daily sales per shop per product)
   - Use UPSERT logic to keep dimensions updated.

---

## 3️⃣ Star Schema Design

- **dim_date**: `dateid`, `fulldate`, `year`, `quarter`, `month`, `day`
- **dim_shop**: `shop_key`, `shopid_src`, `shop_name`, `city_name`, `region_name`, `country_name`
- **dim_product**: `product_key`, `articleid_src`, `article_name`, `price_eur`, `product_group_name`, `product_family_name`, `product_category_name`
- **fact_sales_star**: `dateid`, `shop_key`, `product_key`, `quantity`, `turnover_eur`

---

## 4️⃣ Power BI Report
The Power BI report connects to the PostgreSQL DWH and visualizes sales data using a **Matrix** visual with drill-down capabilities.

### Features:
- **Rows**: Region → Quarter (drill-down enabled)
- **Columns**: Product names
- **Values**: 
  - `Total Quantity` = SUM of sold units
  - `Total Revenue` = SUM of turnover (€)
- **Subtotals** and **Grand totals** enabled
- Interactive drill-down / roll-up navigation
- Ability to filter by time period or product category

### Example:
![Power BI Matrix Example](screenshot.png)

---

## 5️⃣ Technologies Used
- **PostgreSQL** (Data Warehouse)
- **Python** (ETL: pandas, psycopg2)
- **Power BI Desktop** (Data visualization & analysis)

---

## 6️⃣ How to Run
1. Execute `stores-and-products.sql` in PostgreSQL.
2. Run `etl_process.py` to populate the star schema.
3. Open `sales_analysis.pbix` in Power BI Desktop and refresh the data.
4. Use drill-down features in the Matrix visual to explore data by Region, Quarter, and Product.


