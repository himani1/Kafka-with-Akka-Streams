package demo;


import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class KafkaConsumer {
    
    private static final Config CONF = ConfigFactory.load();
    
    private static final ActorSystem system = ActorSystem.create("ConsumerExampleTest");
    
    private static final Materializer materializer = ActorMaterializer.create(system);
    
    public static void main(String[] args) throws Exception {
        
        final Config config = CONF.getConfig("akka.kafka.consumer");
        
        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("group1")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // #atMostOnce
        Consumer.Control control =
                Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics(CONF.getString("kafka.topic")))
                        .mapAsync(10, record -> {
                            System.out.println("record fetched is:: " + record.value());
                            return business(String.valueOf(record.key()), String.valueOf(record.value()));
                        })
                        .to(Sink.foreach(it -> System.out.println("Done with " + it)))
                        .run(materializer);
        
        System.out.println(control.isShutdown());
    }
    
    private static CompletionStage<String> business(String key, String value) { // .... }
        // #atMostOnce #atLeastOnce
        return CompletableFuture.completedFuture("");
    }
    
    
}
