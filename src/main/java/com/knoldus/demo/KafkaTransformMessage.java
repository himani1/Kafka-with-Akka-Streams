package com.knoldus.demo;


import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class KafkaTransformMessage {
    
    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "test-kafka-with-akka-streams-topic-v11";
    private static final String KAFKA_TRANSFORMED_MSG_TOPIC = "topic-21";
    
    
    public static void main(String[] args) {
        
        final ActorSystem system = ActorSystem.create("TransformerExampleTest");
        
        final Materializer materializer = ActorMaterializer.create(system);
        
        final ProducerSettings<String, String> producerSettings =
                ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers(KAFKA_SERVERS);
        
        
        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("group12")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        Consumer.committableSource(consumerSettings, Subscriptions.topics(KAFKA_TOPIC))
                .map(
                        msg ->
                                ProducerMessage.<String, String, ConsumerMessage.Committable>single(
                                        new ProducerRecord<String, String>(KAFKA_TRANSFORMED_MSG_TOPIC, msg.record()
                                                .key().toString(), msg.record()
                                                .value().toString() + "_knoldus"),
                                        msg.committableOffset()))
                .toMat(Producer.committableSink(producerSettings), Keep.both())
                .run(materializer);
    }
    
    private static CompletionStage<String> business(String key, String value) { // .... }
        // #atMostOnce #atLeastOnce
        return CompletableFuture.completedFuture("");
    }
}
