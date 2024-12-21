# Kafka Commands

### Command 1: Create a Kafka Topic
```bash
sudo docker exec kafka kafka-topics.sh --create --topic test --bootstrap-server 135.125.183.126:9092 --partitions 1 --replication-factor 1
```

### Command 2: List Kafka Topics
```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Command 3: Consume Messages from Kafka Topic
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic gfg_topic --from-beginning
```

