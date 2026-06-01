from pyflink.table import StreamTableEnvironment, EnvironmentSettings

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(environment_settings=settings)

table_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///opt/flink/usrlib/flink-sql-connector-kafka-1.17.0.jar"
)

# Source: orders
table_env.execute_sql("""
    CREATE TABLE source_orders (
        order_id STRING,
        customer_id STRING,
        order_status STRING,
        order_purchase_timestamp TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'demo.public.orders',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink-orders-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'debezium-json'
    )
""")

# Source: order_items
table_env.execute_sql("""
    CREATE TABLE source_order_items (
        order_id STRING,
        product_id STRING,
        price DOUBLE,
        freight_value DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'demo.public.order_items',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink-items-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'debezium-json'
    )
""")

# Sink: Kafka topic demo.processed.sales
table_env.execute_sql("""
    CREATE TABLE sink_sales (
        order_id STRING,
        customer_id STRING,
        order_status STRING,
        product_id STRING,
        price DOUBLE,
        freight_value DOUBLE,
        order_purchase_timestamp TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'demo.processed.sales',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format' = 'json'
    )
""")

# Join + insert
table_env.execute_sql("""
    INSERT INTO sink_sales
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        i.product_id,
        i.price,
        i.freight_value,
        o.order_purchase_timestamp
    FROM source_orders o
    JOIN source_order_items i ON o.order_id = i.order_id
""")