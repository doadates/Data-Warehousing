import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from psycopg2 import sql

# =========================
# PostgreSQL bağlantısı
# =========================
conn = psycopg2.connect(
    dbname="dis-2025",
    user="vsisp06",
    password="mb6HbaLL",
    host="vsisdb.informatik.uni-hamburg.de",
    port="5432"
)
conn.autocommit = False
cur = conn.cursor()

print("[E] Connection established.")

# =========================
# CSV'yi oku
# =========================
print("[E] Reading CSV ...")
sales_df = pd.read_csv(
    r"C:\Users\User\Desktop\Database\task 6\ressources\sales.csv",
    encoding="latin1",
    sep=';',
    on_bad_lines='skip' 
)

# 1) Kolon adlarını normalize et ve verilenle eşle
rename_map = {
    'Date': 'date',
    'Shop': 'shop_name',
    'Article': 'article_name',
    'Sold': 'quantity',
    'Revenue': 'turnover'
}
sales_df.rename(columns=rename_map, inplace=True)
sales_df.columns = [c.strip().lower() for c in sales_df.columns]

# 2) Tarih ve sayı dönüşümleri
print("[T] Casting/cleaning date & numbers ...")
sales_df['date'] = pd.to_datetime(sales_df['date'], errors='coerce')
sales_df = sales_df.dropna(subset=['date', 'shop_name', 'article_name'])

# Turnover virgül -> nokta ve float
sales_df['turnover'] = (
    sales_df['turnover']
      .astype(str)
      .str.replace(".", "", regex=False)  # 1.234,56 -> 1234,56
      .str.replace(",", ".", regex=False) # 1234,56 -> 1234.56
)
sales_df['turnover'] = pd.to_numeric(sales_df['turnover'], errors='coerce')
sales_df['quantity'] = pd.to_numeric(sales_df['quantity'], errors='coerce')

# Negatif/NaN temizle
before = len(sales_df)
sales_df = sales_df.dropna(subset=['quantity', 'turnover'])
sales_df = sales_df[(sales_df['quantity'] >= 0) & (sales_df['turnover'] >= 0)]
after = len(sales_df)
print(f"[T] Dropped {before - after} invalid rows.")

# =========================
# DWH Şema: Star Schema tabloları
# =========================
print("[L] Ensuring DWH (star) tables exist ...")

ddl_statements = [
    # Tarih boyutu (DateID surrogate key, FullDate unique)
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        dateid      SERIAL PRIMARY KEY,
        fulldate    DATE UNIQUE,
        day         INT NOT NULL,
        month       INT NOT NULL,
        quarter     INT NOT NULL,
        year        INT NOT NULL
    );
    """,
    # Mağaza boyutu (ShopKey surrogate, ShopID_src unique)
    """
    CREATE TABLE IF NOT EXISTS dim_shop (
        shop_key        BIGSERIAL PRIMARY KEY,
        shopid_src      INT UNIQUE,
        shop_name       VARCHAR(255) NOT NULL,
        city_name       VARCHAR(255) NOT NULL,
        region_name     VARCHAR(255) NOT NULL,
        country_name    VARCHAR(255) NOT NULL
    );
    """,
    # Ürün boyutu (ProductKey surrogate, ArticleID_src unique)
    """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key             BIGSERIAL PRIMARY KEY,
        articleid_src           INT UNIQUE,
        article_name            VARCHAR(255) NOT NULL,
        price_eur               NUMERIC(12,2) NOT NULL,
        product_group_name      VARCHAR(255) NOT NULL,
        product_family_name     VARCHAR(255) NOT NULL,
        product_category_name   VARCHAR(255) NOT NULL
    );
    """,
    # Fact (günlük mağaza-ürün)
    """
    CREATE TABLE IF NOT EXISTS fact_sales_star (
        dateid      INT NOT NULL REFERENCES dim_date(dateid),
        shop_key    BIGINT NOT NULL REFERENCES dim_shop(shop_key),
        product_key BIGINT NOT NULL REFERENCES dim_product(product_key),
        quantity    INT NOT NULL CHECK (quantity >= 0),
        turnover_eur NUMERIC(14,2) NOT NULL CHECK (turnover_eur >= 0),
        load_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (dateid, shop_key, product_key)
    );
    """,
    # Performans indeks önerileri
    "CREATE INDEX IF NOT EXISTS ix_fact_shop ON fact_sales_star(shop_key);",
    "CREATE INDEX IF NOT EXISTS ix_fact_product ON fact_sales_star(product_key);"
]

for ddl in ddl_statements:
    cur.execute(ddl)
conn.commit()
print("[L] Star schema tables ready.")

# =========================
# Kaynak OLTP'den boyutları flatten et
# =========================
print("[T] Building flattened dimensions from OLTP ...")

# Shop -> City -> Region -> Country
cur.execute("""
    SELECT s.shopid AS shopid_src, s.name AS shop_name,
           c.name AS city_name, r.name AS region_name, co.name AS country_name
    FROM shop s
    JOIN city c   ON s.cityid = c.cityid
    JOIN region r ON c.regionid = r.regionid
    JOIN country co ON r.countryid = co.countryid;
""")

rows = cur.fetchall()
dim_shop_df = pd.DataFrame(rows, columns=["shopid_src","shop_name","city_name","region_name","country_name"])

# Article -> ProductGroup -> ProductFamily -> ProductCategory
cur.execute("""
    SELECT a.articleid AS articleid_src,
           a.name AS article_name,
           a.price AS price_eur,
           pg.name AS product_group_name,
           pf.name AS product_family_name,
           pc.name AS product_category_name
    FROM article a
    JOIN productgroup pg   ON a.productgroupid = pg.productgroupid
    JOIN productfamily pf  ON pg.productfamilyid = pf.productfamilyid
    JOIN productcategory pc ON pf.productcategoryid = pc.productcategoryid;
""")

rows = cur.fetchall()
dim_product_df = pd.DataFrame(rows, columns=[
    "articleid_src","article_name","price_eur",
    "product_group_name","product_family_name","product_category_name"
])

print("[T] Flattened dims prepared.")

# =========================
# dim_shop UPSERT (Type-1)
# =========================
print("[L] Upserting dim_shop ...")
upsert_shop_sql = """
INSERT INTO dim_shop (shopid_src, shop_name, city_name, region_name, country_name)
VALUES %s
ON CONFLICT (shopid_src) DO UPDATE SET
  shop_name = EXCLUDED.shop_name,
  city_name = EXCLUDED.city_name,
  region_name = EXCLUDED.region_name,
  country_name = EXCLUDED.country_name;
"""
shop_values = [
    (int(r.shopid_src), r.shop_name, r.city_name, r.region_name, r.country_name)
    for _, r in dim_shop_df.iterrows()
]
if shop_values:
    execute_values(cur, upsert_shop_sql, shop_values, page_size=1000)
    conn.commit()
print("[L] dim_shop done.")

# =========================
# dim_product UPSERT (Type-1)
# =========================
print("[L] Upserting dim_product ...")
upsert_product_sql = """
INSERT INTO dim_product (articleid_src, article_name, price_eur, product_group_name, product_family_name, product_category_name)
VALUES %s
ON CONFLICT (articleid_src) DO UPDATE SET
  article_name = EXCLUDED.article_name,
  price_eur = EXCLUDED.price_eur,
  product_group_name = EXCLUDED.product_group_name,
  product_family_name = EXCLUDED.product_family_name,
  product_category_name = EXCLUDED.product_category_name;
"""
prod_values = [
    (int(r.articleid_src), r.article_name, float(r.price_eur),
     r.product_group_name, r.product_family_name, r.product_category_name)
    for _, r in dim_product_df.iterrows()
]
if prod_values:
    execute_values(cur, upsert_product_sql, prod_values, page_size=1000)
    conn.commit()
print("[L] dim_product done.")

# =========================
# CSV'deki adları Article/Shop ID'lerine map et (adı → kaynak ID → surrogate key)
# =========================
print("[T] Matching CSV names to source IDs ...")

# Article adından id bul
cur.execute('SELECT articleid, name FROM article;')
art_rows = cur.fetchall()
article_df = pd.DataFrame(art_rows, columns=["articleid","name"])
# basit eşleşme (birebir), istersen lower/strip normalizasyonu ekleyebilirsin
sales_df = sales_df.merge(article_df, how='left', left_on='article_name', right_on='name')
sales_df.rename(columns={'articleid':'articleid_src'}, inplace=True)
sales_df.drop(columns=['name'], inplace=True)

missing_prod = sales_df[sales_df['articleid_src'].isna()]
if not missing_prod.empty:
    print("[WARN] Unmatched articles. Sample:")
    print(missing_prod[['article_name']].drop_duplicates().head())

# Shop adından id bul
cur.execute('SELECT shopid, name FROM shop;')
shop_rows = cur.fetchall()
shop_df = pd.DataFrame(shop_rows, columns=["shopid","shop_name"])
sales_df = sales_df.merge(shop_df, how='left', on='shop_name')  # getirir: shopid

missing_shop = sales_df[sales_df['shopid'].isna()]
if not missing_shop.empty:
    print("[WARN] Unmatched shops. Sample:")
    print(missing_shop[['shop_name']].drop_duplicates().head())

# Surrogate key eşlemeleri (dim_shop, dim_product)
cur.execute("SELECT shop_key, shopid_src FROM dim_shop;")
map_shop = pd.DataFrame(cur.fetchall(), columns=["shop_key","shopid_src"])

cur.execute("SELECT product_key, articleid_src FROM dim_product;")
map_prod = pd.DataFrame(cur.fetchall(), columns=["product_key","articleid_src"])

sales_df = sales_df.merge(map_shop, how='left', left_on='shopid', right_on='shopid_src')
sales_df = sales_df.merge(map_prod, how='left', on='articleid_src')

missing_keys = sales_df[sales_df['shop_key'].isna() | sales_df['product_key'].isna()]
if not missing_keys.empty:
    print(f"[WARN] {len(missing_keys)} rows without surrogate keys will be skipped.")

sales_df = sales_df.dropna(subset=['shop_key','product_key'])

# =========================
# dim_date UPSERT + DateID map
# =========================
print("[T] Building/upserting dim_date ...")
dates = pd.DataFrame({'fulldate': pd.to_datetime(sales_df['date'].dt.date).drop_duplicates()})
dates['day'] = dates['fulldate'].dt.day
dates['month'] = dates['fulldate'].dt.month
dates['quarter'] = ((dates['month'] - 1)//3 + 1).astype(int)
dates['year'] = dates['fulldate'].dt.year

# Upsert dim_date satır satır (az sayıda olur)
for _, r in dates.iterrows():
    cur.execute("""
        INSERT INTO dim_date (fulldate, day, month, quarter, year)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (fulldate) DO NOTHING;
    """, (r['fulldate'], int(r['day']), int(r['month']), int(r['quarter']), int(r['year'])))
conn.commit()

# DateID eşleme tablosu
cur.execute("SELECT dateid, fulldate FROM dim_date;")
date_map = pd.DataFrame(cur.fetchall(), columns=["dateid","fulldate"])
date_map['fulldate'] = pd.to_datetime(date_map['fulldate']).dt.date

sales_df['date_only'] = sales_df['date'].dt.date
sales_df = sales_df.merge(date_map, how='left', left_on='date_only', right_on='fulldate')
sales_df = sales_df.drop(columns=['fulldate'])

# =========================
# Fact yükleme (aggregate + UPSERT)
# =========================
print("[L] Loading fact_sales_star ...")

fact_df = (sales_df
           .groupby(['dateid','shop_key','product_key'], as_index=False)
           .agg(quantity=('quantity','sum'),
                turnover_eur=('turnover','sum')))

# UPSERT için execute_values with ON CONFLICT
upsert_fact_sql = """
INSERT INTO fact_sales_star (dateid, shop_key, product_key, quantity, turnover_eur)
VALUES %s
ON CONFLICT (dateid, shop_key, product_key) DO UPDATE SET
  quantity = EXCLUDED.quantity,
  turnover_eur = EXCLUDED.turnover_eur,
  load_ts = now();
"""

fact_values = [
    (int(r.dateid), int(r.shop_key), int(r.product_key), int(r.quantity), float(r.turnover_eur))
    for _, r in fact_df.iterrows()
]
if fact_values:
    execute_values(cur, upsert_fact_sql, fact_values, page_size=1000)
    conn.commit()

cur.close()
conn.close()
print("[DONE] ETL (star schema) finished successfully.")
