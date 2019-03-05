package demo;


import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.CompletionStage;

public class KafkaProducer {
    
    private static final Config CONF = ConfigFactory.load();
    
    private static final ActorSystem system = ActorSystem.create("ProducerExampleTest");
    private static final Materializer materializer = ActorMaterializer.create(system);
    
    public static void main(String[] args) throws Exception {
        
        final Config config = CONF.getConfig("akka.kafka.producer");
        final ProducerSettings<String, String> producerSettings =
                ProducerSettings.create(config, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers(CONF.getString("kafka.servers"));
        
        CompletionStage<Done> done =
                Source.range(1, 100)
                        .map(Object::toString)
                        .map(value -> new ProducerRecord<String, String>(CONF.getString("kafka.topic"), value))
                        .runWith(Producer.plainSink(producerSettings), materializer);
        
    }
    
}
