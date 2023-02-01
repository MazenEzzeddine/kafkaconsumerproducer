import java.io.IOException;
import java.net.URISyntaxException;

public class KafkaConsumerTestAssignor {

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        Thread consumer = new Thread  (new ConsumerThread2());
        Thread server = new Thread  (new ServerThread());
        consumer.start();
        server.start();
        consumer.join();

    }
}




