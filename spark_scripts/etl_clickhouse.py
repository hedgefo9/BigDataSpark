from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_ClickHouse_Reports") \
        .getOrCreate()

def read_star_schema_from_postgres(spark):
    pg_url = "jdbc:postgresql://postgres:5432/sales_db"
    pg_props = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    dim_product = spark.read \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "star_schema.dim_product") \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()

    dim_customer = spark.read \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "star_schema.dim_customer") \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()

    dim_store = spark.read \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "star_schema.dim_store") \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()

    dim_supplier = spark.read \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "star_schema.dim_supplier") \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()

    dim_date = spark.read \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "star_schema.dim_date") \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()

    fact_sales = spark.read \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "star_schema.fact_sales") \
        .option("user", pg_props["user"]) \
        .option("password", pg_props["password"]) \
        .option("driver", pg_props["driver"]) \
        .load()

    return {
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_store": dim_store,
        "dim_supplier": dim_supplier,
        "dim_date": dim_date,
        "fact_sales": fact_sales
    }

def write_to_clickhouse(df, table_name, order_by):
    ch_url = "jdbc:clickhouse://clickhouse:8123/default"
    ch_props = {
        "user": "admin",
        "password": "admin",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

    df.write \
      .format("jdbc") \
      .option("url", ch_url) \
      .option("dbtable", table_name) \
      .option("user", ch_props["user"]) \
      .option("password", ch_props["password"]) \
      .option("driver", ch_props["driver"]) \
      .option("createTableOptions", f"ENGINE = MergeTree() ORDER BY {order_by}") \
      .mode("overwrite") \
      .save()

    print(f"Данные записаны в ClickHouse: {table_name} (ORDER BY {order_by})")

def build_report_products(dim_product, fact_sales):
    df = fact_sales.join(dim_product, "product_key") \
        .groupBy(
            col("product_id"),
            col("name").alias("product_name"),
            col("category").alias("product_category"),
            col("rating").alias("avg_rating"),
            col("reviews").alias("total_reviews")
        ) \
        .agg(
            _sum(col("quantity")).alias("total_quantity"),
            _sum(col("total_price")).alias("total_revenue")
        )
    return df

def build_report_customers(dim_customer, fact_sales):
    df = fact_sales.join(dim_customer, "customer_key") \
        .groupBy(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("country")
        ) \
        .agg(
            _sum(col("total_price")).alias("total_spent"),
            count(col("sale_id")).alias("orders_count"),
            avg(col("total_price")).alias("avg_order_value")
        )
    return df

def build_report_time(dim_date, fact_sales):
    df = fact_sales.join(dim_date, "date_key") \
        .groupBy(
            col("year"),
            col("month")
        ) \
        .agg(
            _sum(col("quantity")).alias("total_quantity"),
            _sum(col("total_price")).alias("total_revenue"),
            avg(col("total_price")).alias("avg_order_value")
        )
    return df

def build_report_stores(dim_store, fact_sales):
    df = fact_sales.join(dim_store, "store_key") \
        .groupBy(
            col("store_key"),
            col("store_name"),
            col("city"),
            col("country")
        ) \
        .agg(
            _sum(col("total_price")).alias("total_revenue"),
            count(col("sale_id")).alias("total_sales"),
            avg(col("total_price")).alias("avg_check")
        )
    return df

def build_report_suppliers(dim_supplier, fact_sales, dim_product):
    # Сначала соединяем fact_sales с dim_supplier по supplier_key
    sales_sup = fact_sales.join(dim_supplier, "supplier_key")
    # Затем присоединяем dim_product, чтобы получить цену («price») конкретного товара
    sales_sup_prod = sales_sup.join(dim_product.select("product_key", "price"), "product_key")

    df = sales_sup_prod.groupBy(
            col("supplier_key"),
            col("supplier_name"),
            col("country")
        ) \
        .agg(
            _sum(col("total_price")).alias("total_revenue"),
            avg(col("price")).alias("avg_price")
        )
    return df

def build_report_quality(dim_product, fact_sales):
    df = fact_sales.join(dim_product, "product_key") \
        .groupBy(
            col("product_id"),
            col("name").alias("product_name"),
            col("rating").alias("avg_rating"),
            col("reviews").alias("total_reviews")
        ) \
        .agg(
            _sum(col("quantity")).alias("total_quantity")
        )
    return df

def main():
    spark = create_spark_session()

    tables = read_star_schema_from_postgres(spark)
    dim_product = tables["dim_product"]
    dim_customer = tables["dim_customer"]
    dim_store = tables["dim_store"]
    dim_supplier = tables["dim_supplier"]
    dim_date = tables["dim_date"]
    fact_sales = tables["fact_sales"]

    # 1. Репорт: продукты (ORDER BY product_id)
    products_df = build_report_products(dim_product, fact_sales)
    write_to_clickhouse(products_df, "report_products", "product_id")

    # 2. Репорт: клиенты (ORDER BY customer_id)
    customers_df = build_report_customers(dim_customer, fact_sales)
    write_to_clickhouse(customers_df, "report_customers", "customer_id")

    # 3. Репорт: время (ORDER BY (year, month))
    time_df = build_report_time(dim_date, fact_sales)
    write_to_clickhouse(time_df, "report_time", "(year, month)")

    # 4. Репорт: магазины (ORDER BY store_key)
    stores_df = build_report_stores(dim_store, fact_sales)
    write_to_clickhouse(stores_df, "report_stores", "store_key")

    # 5. Репорт: поставщики (ORDER BY supplier_key)
    suppliers_df = build_report_suppliers(dim_supplier, fact_sales, dim_product)
    write_to_clickhouse(suppliers_df, "report_suppliers", "supplier_key")

    # 6. Репорт: качество продукции (ORDER BY product_id)
    quality_df = build_report_quality(dim_product, fact_sales)
    write_to_clickhouse(quality_df, "report_quality", "product_id")

    print("Все отчеты записаны в ClickHouse.")

if __name__ == "__main__":
    main()
