from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from datetime import datetime

SCHEMA_REGISTRY_URL = "https://c-c9q84e4h5k4qunej8nol.rw.mdb.yandexcloud.net"
BOOTSTRAP_SERVERS = "rc1a-spkpvkc2po2tm410.mdb.yandexcloud.net:9091,rc1b-guoaqpbufa8obitb.mdb.yandexcloud.net:9091,rc1d-gnkmfu52undh2kdd.mdb.yandexcloud.net:9091"
TOPIC = "orders-topic"
KAFKA_USER = "kafka_user"
KAFKA_PASSWORD = "KafkaLab2024!"
SR_USER = "schema_registry_user"
SR_PASSWORD = "SchemaLab2024!"
GROUP_ID = "lab-consumer-group"

def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL, "basic.auth.user.info": f"{SR_USER}:{SR_PASSWORD}"})
    avro_deserializer = AvroDeserializer(sr_client)
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": KAFKA_USER,
        "sasl.password": KAFKA_PASSWORD,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    print("Consumer запущен!")
    msg_count = 0
    try:
        while msg_count < 5:
            msg = consumer.poll(timeout=10.0)
            if msg is None: continue
            if msg.error(): raise Exception(msg.error())
            order = avro_deserializer(msg.value(), None)
            msg_count += 1
            timestamp = datetime.fromtimestamp(order['timestamp'] / 1000)
            print(f"\nСообщение #{msg_count}")
            print(f"   Order ID: {order['order_id']}")
            print(f"   Customer: {order['customer']}")
            print(f"   Amount: ${order['amount']:.2f}")
            print(f"   Time: {timestamp.strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        consumer.close()
        print(f"Получено сообщений: {msg_count}")

if __name__ == "__main__":
    main()
