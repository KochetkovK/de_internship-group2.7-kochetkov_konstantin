import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with psycopg2.connect(
    dbname="test_db", user="admin", password="admin", 
    host="localhost", port=5435) as conn:
    # with-блок откатывает транзакцию если будет брошено исключение
    with conn.cursor() as cursor:
        # Добавим логический столбец, если не существует
        cursor.execute("""
            ALTER TABLE user_logins
            ADD COLUMN IF NOT EXISTS sent_to_kafka BOOLEAN DEFAULT FALSE;
        """)
        # Проверяем есть ли не отправленные данные и отправляем их в кафку
        while True:
            time.sleep(1)
            cursor.execute("""
                SELECT id,
                       username, 
                       event_type, 
                       extract(epoch FROM event_time)
                FROM user_logins
                WHERE sent_to_kafka = FALSE;
            """)
        
            rows = cursor.fetchall()    
    
            for row in rows:
                user_id = row[0]
                data = {
                    "user": row[1],
                    "event": row[2],
                    "timestamp": float(row[3])  # преобразуем Decimal → float
                }
                producer.send("user_events", value=data)
    
                cursor.execute("""
                    UPDATE user_logins
                    SET sent_to_kafka = TRUE
                    WHERE id = %s""", (user_id,))
                conn.commit()
    
                print("Sent:", data)

                time.sleep(0.1)