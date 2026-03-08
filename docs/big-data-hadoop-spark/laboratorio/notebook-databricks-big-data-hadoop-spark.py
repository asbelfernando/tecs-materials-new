# Databricks notebook source
# MAGIC %md
# MAGIC # Laboratorio: Desarrollo de Aplicaciones de Big Data con Spark en Databricks
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Cargar un dataset de transacciones
# MAGIC - Limpiar y transformar datos con PySpark
# MAGIC - Realizar análisis agregados
# MAGIC - Guardar resultados para su consumo posterior

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Librerías

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuración
# MAGIC Ajusta la ruta según dónde hayas cargado el archivo en Databricks.

# COMMAND ----------

file_path = "/FileStore/tables/sample-retail-transactions.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Esquema del dataset

# COMMAND ----------

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("category", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lectura del archivo CSV

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(file_path)
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exploración inicial

# COMMAND ----------

print("Número de registros:", df.count())
df.printSchema()
display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificación de nulos por columna

# COMMAND ----------

null_summary = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

display(null_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Limpieza de datos

# COMMAND ----------

df_clean = (
    df.dropna(subset=["transaction_id", "city", "category", "product", "quantity", "unit_price"])
      .withColumn("city", F.trim(F.col("city")))
      .withColumn("category", F.trim(F.col("category")))
      .withColumn("product", F.trim(F.col("product")))
      .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
)

display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Transformación principal

# COMMAND ----------

df_transformed = df_clean.withColumn(
    "total_amount",
    F.col("quantity") * F.col("unit_price")
)

display(df_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Análisis por categoría

# COMMAND ----------

sales_by_category = (
    df_transformed
    .groupBy("category")
    .agg(
        F.sum("total_amount").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("transaction_id").alias("transactions")
    )
    .orderBy(F.desc("total_sales"))
)

display(sales_by_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Análisis por ciudad

# COMMAND ----------

sales_by_city = (
    df_transformed
    .groupBy("city")
    .agg(
        F.sum("total_amount").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("transaction_id").alias("transactions")
    )
    .orderBy(F.desc("total_sales"))
)

display(sales_by_city)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Productos más vendidos

# COMMAND ----------

top_products = (
    df_transformed
    .groupBy("product")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_amount").alias("total_sales")
    )
    .orderBy(F.desc("total_quantity"), F.desc("total_sales"))
)

display(top_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Ticket promedio

# COMMAND ----------

avg_ticket = df_transformed.agg(F.avg("total_amount").alias("avg_ticket"))
display(avg_ticket)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Ventas por fecha

# COMMAND ----------

sales_by_date = (
    df_transformed
    .groupBy("transaction_date")
    .agg(F.sum("total_amount").alias("daily_sales"))
    .orderBy("transaction_date")
)

display(sales_by_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Top 5 productos por ventas

# COMMAND ----------

top_5_products_by_sales = (
    df_transformed
    .groupBy("product")
    .agg(F.sum("total_amount").alias("total_sales"))
    .orderBy(F.desc("total_sales"))
    .limit(5)
)

display(top_5_products_by_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Ventas mensuales

# COMMAND ----------

monthly_sales = (
    df_transformed
    .withColumn("year_month", F.date_format("transaction_date", "yyyy-MM"))
    .groupBy("year_month")
    .agg(F.sum("total_amount").alias("monthly_sales"))
    .orderBy("year_month")
)

display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Transacciones con alto valor

# COMMAND ----------

high_value_transactions = df_transformed.filter(F.col("total_amount") > 100)
display(high_value_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Crear vista temporal para SQL

# COMMAND ----------

df_transformed.createOrReplaceTempView("transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   category,
# MAGIC   SUM(total_amount) AS total_sales,
# MAGIC   SUM(quantity) AS total_units
# MAGIC FROM transactions
# MAGIC GROUP BY category
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Guardado de resultados

# COMMAND ----------

output_base = "/FileStore/output/big-data-hadoop-spark"

sales_by_category.write.mode("overwrite").parquet(f"{output_base}/sales_by_category")
sales_by_city.write.mode("overwrite").parquet(f"{output_base}/sales_by_city")
top_products.write.mode("overwrite").parquet(f"{output_base}/top_products")
monthly_sales.write.mode("overwrite").parquet(f"{output_base}/monthly_sales")

print("Resultados guardados correctamente en:", output_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Retos propuestos
# MAGIC 1. Calcular ventas por categoría y ciudad al mismo tiempo
# MAGIC 2. Identificar el producto con mayor ingreso total
# MAGIC 3. Obtener el promedio de unidades por transacción
# MAGIC 4. Repetir el análisis usando SQL en lugar de DataFrame API
# MAGIC 5. Exportar resultados a CSV además de Parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20. Cierre
# MAGIC
# MAGIC En este laboratorio aplicaste un flujo básico de ingeniería y análisis de datos con Spark:
# MAGIC
# MAGIC **carga -> limpieza -> transformación -> análisis -> persistencia**