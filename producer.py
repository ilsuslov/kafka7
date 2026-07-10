from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import time, uuid, random

SCHEMA_REGISTRY_URL = "https://c-c9q84e4h5k4qunej8nol.rw.mdb.yandexcloud.net"
BOOTSTRAP_SERVERS = "rc1a-spkpvkc2po2tm410.mdb.yandexcloud.net:9091,rc1b-guoaqpbufa8obitb.mdb.yandexcloud.net:9091,rc1d-gnkmfu52undh2kdd.mdb.yandexcloud.net:9091"
TOPIC = "orders-topic"
KAFKA_USER = "kafka_user"
KAFKA_PASSWORD = "KafkaLab2024!"
SR_USER = "schema_registry_user"
SR_PASSWORD = "SchemaLab2024!"

schema_str = """{"type":"record","name":"Order","namespace":"com.lab.kafka","fields":[{"name":"order_id","type":"string"},{"name":"customer","type":"string"},{"name":"amount","type":"double"},{"name":"timestamp","type":"long"}]}"""

def delivery_report(err, msg):
    if err: print(f"Ошибка: {err}")
    else: print(f"Отправлено: partition={msg.partition()}, offset={msg.offset()}")

def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL, "basic.auth.user.info": f"{SR_USER}:{SR_PASSWORD}"})
    avro_serializer = AvroSerializer(sr_client, schema_str)
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": KAFKA_USER,
        "sasl.password": KAFKA_PASSWORD,
        "ssl.ca.location": "/etc/ssl/certs/ca-certificates.crt",
    }
    producer = Producer(conf)
    print("Producer запущен!")
    customers = ["Alice", "Bob", "Charlie", "Diana"]
    for i in range(5):
        order = {"order_id": str(uuid.uuid4()), "customer": random.choice(customers), "amount": round(random.uniform(50.0, 500.0), 2), "timestamp": int(time.time() * 1000)}
        producer.produce(topic=TOPIC, value=avro_serializer(order, SerializationContext(TOPIC, MessageField.VALUE)), on_delivery=delivery_report)
        print(f"Заказ: {order['customer']} | ${order['amount']}")
        producer.poll(0)
        time.sleep(1)
    producer.flush()
    print("Готово!")

if __name__ == "__main__":
    main()
