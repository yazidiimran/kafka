package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithKeyDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(KafkaProducerWithKeyDemo.class);

        String bootstrapServer = "135.125.183.126:9092";

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "gfg_topic";
            String value = "imran yazidi " + i;
            String key = "id_" + i;

            // Log the Key
            logger.info("Key: " + key);

            // Create a Producer Record with Key
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            // Java Producer with Callback
            producer.send(record, (recordMetadata, e) -> {
                // Executes every time a record successfully sent
                // or an exception is thrown
                if (e == null) {
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.partition() + "\n");
                } else {
                    logger.error("Error while producing ", e);
                }
            }).get(); // Block the .send() to make it synchronous
        }

        // Flush and Close the Producer
        producer.flush();
        producer.close();

    }
}
