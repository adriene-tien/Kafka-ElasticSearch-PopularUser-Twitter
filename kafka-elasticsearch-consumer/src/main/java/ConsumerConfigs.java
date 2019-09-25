import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerConfigs {

    private static String bootstrapServer = "127.0.0.1:9092";
    private static String groupID = "";

    // ElasticSearch BONSAI access credentials/details
    private static String hostname = "";
    private static String username = "";
    private static String password = "";

    public ConsumerConfigs() {}

    public static String getHostname() { return hostname; }

    public static String getUsername() { return username; }

    public static String getPassword() { return password; }

    public static String getBootstrapServer() { return bootstrapServer; }

    public static String getGroupID() { return groupID; }

    private static Properties setConsumerProperties()
    {
        if (getBootstrapServer().isEmpty()
                || getGroupID().isEmpty()
                || getHostname().isEmpty()
                || getUsername().isEmpty()
                || getPassword().isEmpty()
        ) {
            System.out.println("Please make sure Consumer Configuration access details are set properly.");
            System.exit(0);
        }

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServer());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getGroupID());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        return properties;
    }

    public static KafkaConsumer<String, String> createConsumer(String followTopic)
    {
        Properties properties = setConsumerProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(followTopic));
        return consumer;
    }

}
