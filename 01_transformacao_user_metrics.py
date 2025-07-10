# Databricks notebook source
# MAGIC %md
# MAGIC Ingestão, validação e limpeza dos dados

# COMMAND ----------

import requests
import tarfile
import os
from io import BytesIO

# Arquivos e URLs
files = {
    "order.json.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
    "consumer.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
    "restaurant.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
    "ab_test_ref.tar.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
}

# Volume no Unity Catalog
volume_path = "/Volumes/workspace/default/ifood"

# Cria o diretório se não existir
dbutils.fs.mkdirs(f"dbfs:{volume_path}")

# Faz o download dos arquivos
for filename, url in files.items():
    print(f"⬇️ Baixando {filename}...")
    response = requests.get(url)
    
    # Se for o arquivo tar.gz, extrai csv 
        if filename.endswith(".tar.gz"):
        tar_file = tarfile.open(fileobj=BytesIO(response.content), mode="r:gz")
        for member in tar_file.getmembers():
            if member.name.endswith(".csv"):
                extracted_file = tar_file.extractfile(member)
                content = extracted_file.read()
                with open(f"{volume_path}/ab_test_ref.csv", "wb") as f:
                    f.write(content)
                print(f"✅ Extraído e salvo: ab_test_ref.csv")
    else:
        with open(f"{volume_path}/{filename}", "wb") as f:
            f.write(response.content)
        print(f"✅ Salvo: {filename}")


# COMMAND ----------

df_orders = spark.read.option("multiline", True).json("/Volumes/workspace/default/ifood/order.json.gz")
df_consumers = spark.read.option("header", True).csv("/Volumes/workspace/default/ifood/consumer.csv.gz")
df_restaurants = spark.read.option("header", True).csv("/Volumes/workspace/default/ifood/restaurant.csv.gz")
df_ab_test = spark.read.option("header", True).csv("/Volumes/workspace/default/ifood/ab_test_ref.csv")

# COMMAND ----------

df_orders = spark.read.option("multiline", True).json("/Volumes/workspace/default/ifood/order.json.gz")
df_orders.select("order_id", "customer_id", "order_total_amount", "order_created_at").show(5, truncate=False)

from pyspark.sql.functions import col

df_orders_clean = df_orders.select(
    col("order_id"),
    col("customer_id"),
    col("merchant_id"),
    col("order_created_at").cast("timestamp"),
    col("order_total_amount").cast("double"),
    col("items")
).dropna(subset=["order_id", "customer_id", "order_created_at", "order_total_amount"])



# COMMAND ----------

df_consumers = spark.read.option("header", True).csv("/Volumes/workspace/default/ifood/consumer.csv.gz")
df_consumers.select("customer_id", "created_at", "active", "language").show(5, truncate=False)

df_consumers_clean = df_consumers.select(
    col("customer_id"),
    col("created_at").cast("timestamp"),
    col("active").cast("boolean"),
    col("language")
).dropna(subset=["customer_id"])


# COMMAND ----------

df_restaurants = spark.read.option("header", True).csv("/Volumes/workspace/default/ifood/restaurant.csv.gz")
df_restaurants.select("id", "price_range", "average_ticket", "merchant_city").show(5, truncate=False)

df_restaurants_clean = df_restaurants.select(
    col("id").alias("merchant_id"),
    col("price_range").cast("int"),
    col("average_ticket").cast("double"),
    col("merchant_city")
).dropna(subset=["merchant_id"])

# COMMAND ----------

df_ab_test = spark.read.option("header", True).csv("/Volumes/workspace/default/ifood/ab_test_ref.csv")
df_ab_test.select("customer_id", "is_target").show(5, truncate=False)

df_ab_test_clean = df_ab_test.select(
    col("customer_id"),
    col("is_target")
).dropna(subset=["customer_id", "is_target"])


# COMMAND ----------

from pyspark.sql.functions import from_json, col, explode, when
from pyspark.sql.types import *

# Schema dos itens
item_schema = ArrayType(
    StructType([
        StructField("name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unitPrice", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("totalValue", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("discount", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("totalDiscount", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("addition", StructType([
            StructField("value", StringType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("integrationId", StringType(), True)
    ])
)

# Aplica o parsing da coluna items
df_orders_parsed = df_orders_clean.withColumn("items_parsed", from_json("items", item_schema))

# Cria a tabela de itens explodida com os ajustes
# Buscar por decontos/cupons
df_items_flat = df_orders_parsed \
    .select("order_id", "customer_id", explode("items_parsed").alias("item")) \
    .select(
        "order_id",
        "customer_id",
        col("item.name").alias("item_name"),
        col("item.quantity"),
        (when(col("item.unitPrice.value").isNotNull(), col("item.unitPrice.value").cast("double") / 100).otherwise(0)).alias("unit_price"),
        (when(col("item.totalValue.value").isNotNull(), col("item.totalValue.value").cast("double") / 100).otherwise(0)).alias("total_value"),
        (when(col("item.discount.value").isNotNull(), col("item.discount.value").cast("double") / 100).otherwise(0)).alias("item_discount"),
        (when(col("item.totalDiscount.value").isNotNull(), col("item.totalDiscount.value").cast("double") / 100).otherwise(0)).alias("item_total_discount"),
        (when(col("item.addition.value").isNotNull(), col("item.addition.value").cast("double") / 100).otherwise(0)).alias("item_addition"),
        col("item.integrationId").alias("integration_id")
    )


# COMMAND ----------

from pyspark.sql.functions import sum as Fsum, count, max, when

# Agregação por pedido (detalhamento dos itens, desconto)
df_order_summary = df_items_flat.groupBy("order_id", "customer_id").agg(
    Fsum("total_value").alias("order_total_value"),
    Fsum("item_total_discount").alias("order_total_discount"),
    count("*").alias("num_items"),
    max(when(col("item_total_discount") > 0, 1).otherwise(0)).alias("has_coupon_applied")
)

df_orders_enriched = df_order_summary.join(
    df_orders_clean.select("order_id", "merchant_id", "order_created_at"),
    on="order_id",
    how="left"
)


# COMMAND ----------

from pyspark.sql.functions import min as Fmin, datediff

df_first_order = df_orders_enriched.groupBy("customer_id").agg(
    Fmin("order_created_at").alias("first_order_date")
)

df_orders_with_days = df_orders_enriched.join(df_first_order, on="customer_id", how="left") \
    .withColumn("days_since_first", datediff("order_created_at", "first_order_date"))

from pyspark.sql.functions import sum as Fsum, countDistinct, avg, max, when

df_user_metrics = df_orders_with_days.groupBy("customer_id").agg(
    countDistinct("order_id").alias("num_orders_total"),
    Fsum("order_total_value").alias("total_value"),
    Fsum("order_total_discount").alias("total_discount"),
    avg("order_total_value").alias("avg_ticket"),
    max("has_coupon_applied").alias("used_coupon"),
    
    # Retenção binária
    max(when((col("days_since_first") > 0) & (col("days_since_first") <= 3), 1).otherwise(0)).alias("retention_d3"),
    max(when((col("days_since_first") > 0) & (col("days_since_first") <= 7), 1).otherwise(0)).alias("retention_d7"),
    max(when((col("days_since_first") > 0) & (col("days_since_first") <= 14), 1).otherwise(0)).alias("retention_d14"),
    max(when((col("days_since_first") > 0) & (col("days_since_first") <= 30), 1).otherwise(0)).alias("retention_d30")
)

df_user_metrics_abtest = df_user_metrics.join(df_ab_test_clean, on="customer_id", how="left")


# COMMAND ----------

df_user_metrics_abtest.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/ifood/user_metrics_abtest_delta")
