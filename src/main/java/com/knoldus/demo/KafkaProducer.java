package com.knoldus.demo;


import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducer {

    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "test-kafka-with-akka-streams-topic-v11";
    
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("ProducerExampleTest");
        final Materializer materializer = ActorMaterializer.create(system);

        final ProducerSettings<String, String> producerSettings =
                ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers(KAFKA_SERVERS);
        Source.range(1, 100)
                .map(Object::toString)
                .map(value -> new ProducerRecord<String, String>(KAFKA_TOPIC, value))
                .runWith(Producer.plainSink(producerSettings), materializer);
    }
    
}
