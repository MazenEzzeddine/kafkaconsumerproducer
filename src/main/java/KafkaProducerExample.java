import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.Instant.now;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
    private static long iteration = 0;

     static KafkaProducerConfig config;
     static KafkaProducer<String, Customer> producer;
     static Random rnd;
     static long key;
     static int eventsPerSeconds;

   public  static KafkaProducer<String, Customer> producerFactory() throws InterruptedException, IOException, URISyntaxException {
        rnd = new Random();
        config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        producer = new KafkaProducer<String, Customer>(props);
        //log.info("Sending {} messages ...", config.getMessageCount());

        AtomicLong numSent = new AtomicLong(0);
        // over all the workload
        key = 0L;

       return producer;
    }



}








