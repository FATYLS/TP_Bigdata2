# Docker - Commandes utilisées

## Accès au conteneur
docker exec -it hadoop-master bash

## Kafka

### Création du topic
kafka-topics.sh --create --topic Hello-Kafka --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

### Lister les topics
kafka-topics.sh --list --bootstrap-server localhost:9092

### Producer (envoi de messages)
kafka-console-producer.sh --broker-list localhost:9092 --topic Hello-Kafka

### Consumer (lecture des messages)
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Hello-Kafka --from-beginning

## Spark Streaming

### Copier le fichier JAR dans le conteneur
docker cp target/stream-kafka-spark-1-jar-with-dependencies.jar hadoop-master:/root/stream-kafka.jar

### Lancer le traitement Spark Streaming
spark-submit --class spark.kafka.SparkKafkaWordCount --master local stream-kafka.jar localhost:9092 Hello-Kafka mySparkConsumerGroup >> out

### Afficher le résultat
cat out
