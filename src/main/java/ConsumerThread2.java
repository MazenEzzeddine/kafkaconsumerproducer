import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;


public class ConsumerThread2 implements Runnable {
    private static final Logger log = LogManager.getLogger(ConsumerThread2.class);
    static double eventsViolating = 0;
    static double eventsNonViolating = 0;
    static double totalEvents = 0;
    public static KafkaConsumer<String, Customer> consumer = null;
    static float maxConsumptionRatePerConsumer = 0.0f;
    static float ConsumptionRatePerConsumerInThisPoll = 0.0f;
    static float averageRatePerConsumerForGrpc = 0.0f;
    static long pollsSoFar = 0;
    //static Double maxConsumptionRatePerConsumer1 = 0.0d;
    //Long[] waitingTimes = new Long[10];


    static PrometheusMeterRegistry prometheusRegistry;
    static TimeMeasure latencygaugemeasure;
    static Gauge latencygauge;

    ArrayList<TopicPartition> tps;


    KafkaProducer<String, Customer> producer = KafkaProducerExample.producerFactory();

    public ConsumerThread2() throws IOException, URISyntaxException, InterruptedException {
    }

    @Override
    public void run() {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, BinPackPartitionAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LagBasedPartitionAssignor.class.getName());
      /*  props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                org.apache.kafka.clients.consumer.RangeAssignor.class.getName());*/
        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));
        log.info("Subscribed to topic {}", config.getTopic());
       initPrometheus();


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Starting exit...");
                consumer.wakeup();
                try {
                    this.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


      /*  tps = new ArrayList<>();
        tps.add(new TopicPartition("testtopic1", 0));
        tps.add(new TopicPartition("testtopic1", 1));
        tps.add(new TopicPartition("testtopic1", 2));
        tps.add(new TopicPartition("testtopic1", 3));
        tps.add(new TopicPartition("testtopic1", 4));
*/

        try {
            while (true) {
                Long timeBeforePolling = System.currentTimeMillis();
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                if (records.count() != 0) {

                  /*  for (TopicPartition tp : tps) {*/
                        /*    double percenttopic2 = *//*Math.ceil(*//*records.records(tp).size()*//**0.7*//* *//**0.5*//* *//** 0.7*//*;*//*);*//*
                        double currentEventIndex = 0;*/
                        for (ConsumerRecord<String, Customer> record : records) {
                            totalEvents++;
                            if (System.currentTimeMillis() - record.timestamp() <= 5000) {
                                eventsNonViolating++;
                            } else {
                                eventsViolating++;
                            }
                            //TODO sleep per record or per batch
                            try {
                                Thread.sleep(Long.parseLong(config.getSleep()));
                                latencygaugemeasure.setDuration(System.currentTimeMillis() - record.timestamp());

                              /*  if (currentEventIndex < percenttopic2) {


                                }
                                currentEventIndex++;*/

                               /* producer.send(new ProducerRecord<String, Customer>("testtopic2",
                                        tp.partition(), record.timestamp(), record.key(), record.value()));*/

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        if (commit) {
                            consumer.commitSync();
                        }
                        log.info("In this poll, received {} events", records.count());
                        Long timeAfterPollingProcessingAndCommit = System.currentTimeMillis();
                        ConsumptionRatePerConsumerInThisPoll = ((float) records.count() /
                                (float) (timeAfterPollingProcessingAndCommit - timeBeforePolling)) * 1000.0f;
                        pollsSoFar += 1;
                        averageRatePerConsumerForGrpc = averageRatePerConsumerForGrpc +
                                (ConsumptionRatePerConsumerInThisPoll - averageRatePerConsumerForGrpc) / (float) (pollsSoFar);

                        if (maxConsumptionRatePerConsumer < ConsumptionRatePerConsumerInThisPoll) {
                            maxConsumptionRatePerConsumer = ConsumptionRatePerConsumerInThisPoll;
                        }
                        log.info("ConsumptionRatePerConsumerInThisPoll in this poll {}", ConsumptionRatePerConsumerInThisPoll);
                        log.info("maxConsumptionRatePerConsumer {}", maxConsumptionRatePerConsumer);
                        double percentViolating = (double) eventsViolating / (double) totalEvents;
                        double percentNonViolating = (double) eventsNonViolating / (double) totalEvents;
                        log.info("Percent violating so far {}", percentViolating);
                        log.info("Percent non violating so far {}", percentNonViolating);
                        log.info("total events {}", totalEvents);
                    }
                }

        } catch (WakeupException e) {
            // e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }
    }


    private static void initPrometheus() {
        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/prometheus", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        latencygaugemeasure = new TimeMeasure(0.0);
        latencygauge = Gauge.builder("latencygauge", latencygaugemeasure, TimeMeasure::getDuration).register(prometheusRegistry);//prometheusRegistry.gauge("timergauge" );
    }


}
