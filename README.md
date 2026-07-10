 Параметры кластера

Имя кластера	`kafka203`
ID кластера	`c9q84e4h5k4qunej8nol`
Окружение	`PRODUCTION`
Версия Kafka	`3.9.2`
Зоны доступности	`ru-central1-a`, `ru-central1-b`, `ru-central1-d`
Schema Registry	Включён
Ресурсы брокеров	`s3-c2-m8` (2 vCPU, 8 GB RAM), 32 GB SSD
Ресурсы ZooKeeper	`s4a-c2-m8` (2 vCPU, 8 GB RAM), 10 GB SSD

Хосты Kafka (Brokers):

rc1a-spkpvkc2po2tm410.mdb.yandexcloud.net (ru-central1-a)
rc1b-guoaqpbufa8obitb.mdb.yandexcloud.net (ru-central1-b)
rc1d-gnkmfu52undh2kdd.mdb.yandexcloud.net (ru-central1-d)
Порт подключения: 9091 (SASL_SSL)

Топики и пользователи
yc managed-kafka topic list --cluster-name kafka203
+--------------+------------------+--------------------+
|     NAME     | PARTITIONS COUNT | REPLICATION FACTOR |
+--------------+------------------+--------------------+
| orders-topic |                3 |                  3 |
+--------------+------------------+--------------------+

yc managed-kafka user list --cluster-name kafka203
+----------------------+----------------+
|         NAME         |      ROLE      |
+----------------------+----------------+
| kafka_user           | KAFKA_USER     |
| schema_registry_user | SCHEMA_REGISTRY|
+----------------------+----------------+


