from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

# Initialisation
spark = SparkSession.builder \
    .appName("KafkaToClickHouse") \
    .getOrCreate()

# Schéma Debezium (Assurez-vous qu'il correspond exactement à votre JSON)
schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("price", DecimalType(10, 2), True),
            StructField("freight_value", DecimalType(10, 2), True)
        ]), True)
    ]), True)
])

# Lecture depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "demo.public.order_items") \
    .option("startingOffsets", "latest") \
    .load()

# Transformation
clean_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.payload.after.*") \
    .filter(col("order_id").isNotNull()) \
    .withColumn("total_amount", col("price") + col("freight_value")) \
    .select("order_id", "product_id", "total_amount")

# Écriture vers ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    if not batch_df.isEmpty: # Vérification importante pour éviter les batches vides
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
            .option("dbtable", "sales") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

# Lancement du Stream avec checkpoint obligatoire
query = clean_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/sales") \
    .start()

query.awaitTermination()