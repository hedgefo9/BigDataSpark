import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, quarter,
    date_format, dayofweek, weekofyear, coalesce
)

PG_URL = "jdbc:postgresql://postgres:5432/sales_db"
PG_PROPS = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}


def build_dimensions(spark, df):
    dim_customer = (
        df.select(
            col("sale_customer_id").alias("customer_id"),
            col("customer_first_name").alias("first_name"),
            col("customer_last_name").alias("last_name"),
            col("customer_age").alias("age"),
            col("customer_email").alias("email"),
            col("customer_country").alias("country"),
            col("customer_postal_code").alias("postal_code"),
            col("customer_pet_type").alias("pet_type"),
            col("customer_pet_name").alias("pet_name"),
            col("customer_pet_breed").alias("pet_breed")
        )
        .dropDuplicates(["customer_id"])
    )

    dim_customer.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_customer") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()
    print(f"dim_customer: {dim_customer.count()} строк записано.")

    dim_product = (
        df.select(
            col("sale_product_id").alias("product_id"),
            col("product_name").alias("name"),
            col("product_category").alias("category"),
            col("product_price").alias("price"),
            col("product_weight").alias("weight"),
            col("product_color").alias("color"),
            col("product_size").alias("size"),
            col("product_brand").alias("brand"),
            col("product_material").alias("material"),
            col("product_rating").alias("rating"),
            col("product_reviews").alias("reviews"),
            col("supplier_name").alias("supplier_name")
        )
        .dropDuplicates(["product_id"])
    )

    dim_product.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_product") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()
    print(f"dim_product: {dim_product.count()} строк записано.")

    dim_store = (
        df.select(
            col("store_name"),
            col("store_location").alias("location"),
            col("store_city").alias("city"),
            col("store_state").alias("state"),
            col("store_country").alias("country"),
            col("store_phone").alias("phone"),
            col("store_email").alias("email")
        )
        .dropDuplicates(["store_name", "location", "city", "state", "country"])
    )

    dim_store.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_store") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()
    print(f"dim_store: {dim_store.count()} строк записано.")

    dim_supplier = (
        df.select(
            col("supplier_name"),
            col("supplier_contact").alias("contact"),
            col("supplier_email").alias("email"),
            col("supplier_phone").alias("phone"),
            col("supplier_address").alias("address"),
            col("supplier_city").alias("city"),
            col("supplier_country").alias("country")
        )
        .dropDuplicates([
            "supplier_name", "contact", "email", "phone", "address", "city", "country"
        ])
    )

    dim_supplier.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_supplier") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()
    print(f"dim_supplier: {dim_supplier.count()} строк записано.")

    dim_date_full = (
        df.select(col("sale_date_dt").alias("sale_date"))
        .filter(col("sale_date").isNotNull())
        .distinct()
        .withColumn("year", year(col("sale_date")))
        .withColumn("month", month(col("sale_date")))
        .withColumn("day", dayofmonth(col("sale_date")))
    )

    dim_date_for_pg = dim_date_full.select(
        col("sale_date"),
        col("year"),
        col("month"),
        col("day")
    )

    dim_date_for_pg.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_date") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()
    print(f"dim_date: {dim_date_for_pg.count()} строк записано.")


def build_fact(spark, df):
    dim_customer_pd = spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_customer") \
        .options(**PG_PROPS) \
        .load()

    dim_product_pd = spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_product") \
        .options(**PG_PROPS) \
        .load()

    dim_store_pd = spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_store") \
        .options(**PG_PROPS) \
        .load()

    dim_supplier_pd = spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_supplier") \
        .options(**PG_PROPS) \
        .load()

    dim_date_pd = spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.dim_date") \
        .options(**PG_PROPS) \
        .load()

    existing_fact_ids = spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "star_schema.fact_sales") \
        .options(**PG_PROPS) \
        .load() \
        .select("sale_id")

    fact_candidates = df.join(
        dim_customer_pd.select("customer_key", "customer_id"),
        df.sale_customer_id == dim_customer_pd.customer_id,
        "inner"
    ).drop(dim_customer_pd.customer_id)

    fact_candidates = fact_candidates.join(
        dim_product_pd.select("product_key", "product_id"),
        fact_candidates.sale_product_id == dim_product_pd.product_id,
        "inner"
    ).drop(dim_product_pd.product_id)

    fact_candidates = fact_candidates.join(
        dim_store_pd.select("store_key", "store_name", "location", "city", "state", "country"),
        [
            fact_candidates.store_name == dim_store_pd.store_name,
            fact_candidates.store_location == dim_store_pd.location,
            fact_candidates.store_city == dim_store_pd.city,
            fact_candidates.store_state == dim_store_pd.state,
            fact_candidates.store_country == dim_store_pd.country
        ],
        "inner"
    ).drop(
        dim_store_pd.store_name, dim_store_pd.location,
        dim_store_pd.city, dim_store_pd.state, dim_store_pd.country
    )

    fact_candidates = fact_candidates.join(
        dim_supplier_pd.select("supplier_key", "supplier_name", "city", "country"),
        [
            fact_candidates.supplier_name == dim_supplier_pd.supplier_name,
            fact_candidates.supplier_city == dim_supplier_pd.city,
            fact_candidates.supplier_country == dim_supplier_pd.country
        ],
        "inner"
    ).drop(
        dim_supplier_pd.supplier_name, dim_supplier_pd.city, dim_supplier_pd.country
    )

    fact_candidates = fact_candidates.join(
        dim_date_pd.select("date_key", "sale_date"),
        fact_candidates.sale_date_dt == dim_date_pd.sale_date,
        "inner"
    ).drop(dim_date_pd.sale_date)

    fact_full = fact_candidates.select(
        col("id").alias("sale_id"),
        col("customer_key"),
        col("product_key"),
        col("store_key"),
        col("supplier_key"),
        col("date_key"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    ).dropDuplicates(["sale_id"])

    fact_to_insert = fact_full.join(
        existing_fact_ids,
        on="sale_id",
        how="left_anti"
    )

    count_new = fact_to_insert.count()
    if count_new > 0:
        fact_to_insert.write \
            .format("jdbc") \
            .option("url", PG_URL) \
            .option("dbtable", "star_schema.fact_sales") \
            .options(**PG_PROPS) \
            .mode("append") \
            .save()
        print(f"Новых строк в fact_sales записано: {count_new}")
    else:
        print("Новые строки в fact_sales не найдены. Запись пропущена.")


def main():
    spark = SparkSession.builder \
        .appName("ETL_Postgres_Star") \
        .getOrCreate()

    raw_df = spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "public.mock_data") \
        .options(**PG_PROPS) \
        .load()

    df = raw_df.withColumn(
        "sale_date_dt",
        coalesce(
            to_date(col("sale_date"), "M/d/yyyy"),
            to_date(col("sale_date"), "yyyy-MM-dd")
        )
    )

    build_dimensions(spark, df)

    build_fact(spark, df)

    spark.stop()
    print("ETL завершён успешно.")


if __name__ == "__main__":
    main()
