import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

public class ProducerConfigs {

    private static String producerBootstrapServer = "127.0.0.1:9092";
    private static String consumerKey = "";
    private static String consumerSecret = "";
    private static String token = "";
    private static String secret = "";
    private static List<String> followTerms = Lists.newArrayList();

    public ProducerConfigs() {}

    public static String getProducerBootstrapServer()
    {
        return producerBootstrapServer;
    }

    public static String getConsumerKey()
    {
        return consumerKey;
    }

    public static String getConsumerSecret()
    {
        return consumerSecret;
    }

    public static String getToken()
    {
        return token;
    }

    public static String getSecret()
    {
        return secret;
    }

    public static List<String> getFollowTerms()
    {
        return followTerms;
    }

    private static Properties setProducerProperties()
    {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getProducerBootstrapServer());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a safe producer: idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so keep this as 5, use 1 otherwise

        // create a high throughput producer (at the expense of latency and some CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // snappy is efficient, helpful for text msg types, JSON
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 kb size

        return properties;
    }

    public static KafkaProducer<String, String> createProducer()
    {
        Properties properties = setProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private static List<String> getDesiredFollowTerms()
    {
        System.out.println("Please enter the terms you wish to track, delimited by a newline / linebreak. When finished, enter \"!q\": ");
        String userEnteredTerm = "";
        Scanner desiredFollowTermsFromUser = new Scanner(System.in);
        while (true) {
            userEnteredTerm = desiredFollowTermsFromUser.next();
            if (!userEnteredTerm.isEmpty() && !userEnteredTerm.equals("!q")) {
                followTerms.add(userEnteredTerm);
            } else {
                break;
            }
        }

        return ProducerConfigs.getFollowTerms();
    }

    public static Client createTwitterClient(BlockingQueue<String> msgQueue)
    {
        ProducerConfigs.getDesiredFollowTerms();

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(getFollowTerms());

        // consider working with all credential vars in a config file instead
        Authentication hosebirdAuth = new OAuth1(getConsumerKey(), getConsumerSecret(), getToken(), getSecret());

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }


}
