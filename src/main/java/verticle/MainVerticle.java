package verticle;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class MainVerticle {

    Logger log = Logger.getLogger(MainVerticle.class.getName());

    private final static String TOPIC_NAME = "test.topic";
    private final static int SLEEP_TIME = 1000 * 60 * 10; //10 minutes, topic creates after 5

    public static void main(String...args) throws InterruptedException, ExecutionException {
        Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "test.value");
        producer.send(record).get();
        producer.flush();

        AdminClient admin = createAdmin();
        admin.deleteTopics(Collections.singletonList(TOPIC_NAME));
        Thread.sleep(1000 * 60 * 10);
    }


    private static AdminClient createAdmin() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:29092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return KafkaAdminClient.create(props);
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:29092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }


}
