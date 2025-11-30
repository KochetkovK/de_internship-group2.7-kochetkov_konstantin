from kafka import KafkaConsumer
import json
import clickhouse_connect

consumer = KafkaConsumer(    
    "user_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="user-logins-consumer",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = clickhouse_connect.get_client(host='localhost', port=8124, username='user', password='strongpassword')

client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
""")

for message in consumer:
    data = message.value
    print("Received:", data)
    client.command(
        f"INSERT INTO user_logins (username, event_type, event_time) VALUES ('{data['user']}', '{data['event']}', toDateTime({data['timestamp']}))"
    )