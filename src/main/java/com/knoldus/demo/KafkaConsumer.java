package com.knoldus.demo;


import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class KafkaConsumer {

    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "test-kafka-with-akka-streams-topic-v11";
    private static final String KAFKA_TRANSFORMED_MSG_TOPIC = "topic-21";


    private static final ActorSystem system = ActorSystem.create("ConsumerExampleTest");
    
    private static final Materializer materializer = ActorMaterializer.create(system);
    
    public static void main(String[] args) {
        

        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers(KAFKA_SERVERS)
                        .withGroupId("group1")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // #atMostOnce
        Consumer.Control control =
                Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics(KAFKA_TRANSFORMED_MSG_TOPIC))
                        .mapAsync(10, record -> {
                            System.out.println("record fetched is:: " + record.value());
                            return business(String.valueOf(record.key()), String.valueOf(record.value()));
                        })
                        .to(Sink.foreach(it -> System.out.println("Done with " + it)))
                        .run(materializer);
        
    }
    
    private static CompletionStage<String> business(String key, String value) { // .... }
        // #atMostOnce #atLeastOnce
        return CompletableFuture.completedFuture("");
    }
    
    
}
