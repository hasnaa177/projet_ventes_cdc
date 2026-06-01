from kafka import KafkaConsumer
import clickhouse_connect
import json

consumer = KafkaConsumer(
    'demo.public.order_items',
    bootstrap_servers='kafka:29092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = clickhouse_connect.get_client(host='localhost', port=8123)

print("Consuming...")
for msg in consumer:
    data = msg.value.get('payload', {}).get('after', {})
    if not data or not data.get('order_id'):
        continue
    try:
        client.command(f"""
            INSERT INTO sales VALUES (
                '{data.get("order_id","")}', '',  '',
                '{data.get("product_id","")}',
                {float(data.get("price") or 0)},
                {float(data.get("freight_value") or 0)},
                now()
            )
        """)
        print(f"Inserted: {data.get('order_id')}")
    except Exception as e:
        print(f"Error: {e}")
EOF
python3 /tmp/kafka_to_clickhouse.py