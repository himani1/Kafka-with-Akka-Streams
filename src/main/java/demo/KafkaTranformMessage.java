package demo;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Keep;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.kafka.javadsl.Consumer.createDrainingControl;

public class KafkaTranformMessage {
    
    private static final Config CONF = ConfigFactory.load();
    
    private static final ActorSystem system = ActorSystem.create("ConsumerExampleTest");
    
    private static final Materializer materializer = ActorMaterializer.create(system);
    
    public static void main(String[] args) throws Exception {
    
        final Config producerConfig = CONF.getConfig("akka.kafka.consumer");
    
        final ProducerSettings<String, String> producerSettings =
                ProducerSettings.create(producerConfig, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers(CONF.getString("kafka.servers"));
        
        final Config config = CONF.getConfig("akka.kafka.consumer");
        
        CommitterSettings committerSettings = CommitterSettings.create(config);
        
        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("group12")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        Object control =
                Consumer.committableSource(consumerSettings, Subscriptions.topics(CONF.getString("kafka.topic")))
                        .map(
                                msg ->
                                        ProducerMessage.<String, String, ConsumerMessage.Committable>single(
                                                new ProducerRecord<String, String>("topic-2", msg.record().key(), msg.record()
                                                        .value
                                                        ()),
                                                msg.committableOffset()))
                        .toMat(Producer.committableSink(producerSettings), Keep.both())
//                        .mapMaterializedValue(pair -> )
                        .run(materializer);
    }
    
    private static CompletionStage<String> business(String key, String value) { // .... }
        // #atMostOnce #atLeastOnce
        return CompletableFuture.completedFuture("");
    }
}
