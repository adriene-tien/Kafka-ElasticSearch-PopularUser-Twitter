import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private static JsonParser jsonParser = new JsonParser();
    private static String mostPopularUsersTweetDetails[][] = new String[1000][2];
    private static Integer userCountLimit = 0;

    public ElasticSearchConsumer() {}

    public static void main(String[] args) throws IOException { new ElasticSearchConsumer().run(); }

    /**
     *
     * @return
     */
    private static RestHighLevelClient createClient()
    {
        if (ConsumerConfigs.getHostname().isEmpty()
                || ConsumerConfigs.getUsername().isEmpty()
                || ConsumerConfigs.getPassword().isEmpty()
        ) {
            logger.info("Please make sure the access details has been set properly: username, password, hostname, ...");
            logger.info("Exiting...");
            System.exit(0);
        }

        // Using Bonsai, which has some security settings - need credentials for this
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(
                        ConsumerConfigs.getUsername(),
                        ConsumerConfigs.getPassword()
                ));

        // RestClientBuilder: connect through HTTPS to the hostname over port 443
        // Encrypted connection to cloud
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(ConsumerConfigs.getHostname(), 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    /**
     *
     * @param field
     * @param tweetJson
     * @return
     */
    private static String extractFieldFromTweet(String field, String tweetJson)
    {
        // followers_count, favorite_count, retweet_count, reply_count are part of main JSON object
        String fieldData = "";

        if (field == "followers_count") {
            JsonElement getUserElement = jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user");

            fieldData = getUserElement
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsString();
        } else {
            try {
                fieldData = jsonParser.parse(tweetJson)
                        .getAsJsonObject()
                        .get(field)
                        .getAsString();
            } catch (IllegalStateException e) {
                logger.info("Could not find JSON attribute " + field + " ...");
            }
        }

        return fieldData;
    }

    /**
     *
     * @param tweetJson
     * @param userFollowerCount
     */
    private static void keepTrackOfMostFollowedUsers(
            String tweetJson,
            String userFollowerCount,
            String id,
            RestHighLevelClient client,
            BulkRequest bulkRequest
    ) throws IOException {
        if (userCountLimit < 1000) {
            mostPopularUsersTweetDetails[userCountLimit][0] = tweetJson;
            mostPopularUsersTweetDetails[userCountLimit][1] = userFollowerCount;

            IndexRequest indexRequest = new IndexRequest("tweets");
            indexRequest.id(id);
            indexRequest.source(tweetJson, XContentType.JSON);
            logger.info(id);

            userCountLimit++;
        } else if (Integer.parseInt(userFollowerCount) > Integer.parseInt(mostPopularUsersTweetDetails[0][1])) {
            DeleteRequest deleteRequest = new DeleteRequest("tweets", id);
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);

            mostPopularUsersTweetDetails[0][0] = tweetJson;
            mostPopularUsersTweetDetails[0][1] = userFollowerCount;

            IndexRequest indexRequest = new IndexRequest("tweets");
            indexRequest.id(id);
            indexRequest.source(tweetJson, XContentType.JSON);
            logger.info(id);

            // continue adding to our bulk request
            bulkRequest.add(indexRequest);

            SortUsers.sort2DArrayByColumn(mostPopularUsersTweetDetails, 1);
        }
    }

    private static void run() throws IOException {
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = ConsumerConfigs.createConsumer("twitter_tweets");
        BulkRequest bulkRequest = new BulkRequest();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            // loop over all of our records and add them to a bulk request
            for(ConsumerRecord<String, String> record : records) {

                // aim for idempotent processing to avoid duplicate tweets upon failure of consumer
                // twitter JSON comes with an ID field, use this
                try {
                    String id = extractFieldFromTweet("id_str", record.value());
                    String userFollowerCount = extractFieldFromTweet("followers_count", record.value());
                    ElasticSearchConsumer.keepTrackOfMostFollowedUsers(record.value(), userFollowerCount, id, client, bulkRequest);
                } catch (NullPointerException | IOException e) {
                    logger.info("Bad data – id_str field missing from JSON object: " + record.value());
                }
            }

            if (recordCount > 0) {
                try {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (ActionRequestValidationException e) {
                    logger.info("Committing leftover offsets...");
                    consumer.commitSync();
                    logger.info("Offsets have been committed.");
                    logger.info("Continuing to look for tweets from more popular users...");
                    continue;
                }


                logger.info("Committing the offsets.");
                consumer.commitSync();
                logger.info("Offsets have been committed!");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
