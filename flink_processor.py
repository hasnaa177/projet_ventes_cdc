from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Créer l'environnement de table
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(settings)

# 2. Définir la source Kafka (Lecture du flux CDC)
# On définit le schéma pour correspondre à ce que Debezium envoie dans Kafka
table_env.execute_sql("""
    CREATE TABLE orders_source (
        `after` ROW<order_id STRING, customer_id STRING, order_status STRING>,
        ts_ms BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'demo.public.orders',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 3. Créer une vue simplifiée
table_env.execute_sql("""
    CREATE VIEW orders_clean AS
    SELECT `after`.order_id, `after`.order_status
    FROM orders_source
""")

# 4. Afficher le résultat en temps réel dans les logs
table_env.execute_sql("SELECT order_status, COUNT(order_id) FROM orders_clean GROUP BY order_status").print()