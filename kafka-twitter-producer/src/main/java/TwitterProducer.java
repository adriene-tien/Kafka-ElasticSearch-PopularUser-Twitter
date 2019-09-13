import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private static JsonParser jsonParser = new JsonParser();
    private static final String viralTwitterFields[] = {"favorite_count", "retweet_count", "reply_count"};
    private static HashMap<String, Integer> monthsOfTheYear = new HashMap<String, Integer>();

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private static String extractFieldFromTweet(String field, String tweetJson)
    {
        // followers_count, favorite_count, retweet_count, reply_count are part of main JSON object
        String fieldData = "";
        JsonObject tweetObject = jsonParser.parse(tweetJson).getAsJsonObject();

        if (field == "followers_count") {
            // there exists some tweet json objects where some typical fields are non-existent
            // check if user member exists
            boolean hasField = tweetObject.has("user");

            if (hasField && !tweetObject.isJsonNull() ) {
                // get user json object as string
                JsonObject userObject = tweetObject
                        .get("user")
                        .getAsJsonObject();

                // check if subfield member, in this case "followers_count", exists

                boolean hasSubField = userObject.has(field);

                if (hasSubField && !userObject.isJsonNull()) {
                    // get field result as string
                    fieldData = userObject
                            .get(field)
                            .getAsString(); //get subfield member as string
                }
            }
        } else {
            boolean hasField = tweetObject.has(field);
            if (hasField && !tweetObject.isJsonNull()) {
                try {
                    fieldData = tweetObject
                            .get(field)
                            .getAsString();
                } catch (IllegalStateException | UnsupportedOperationException e) {
                    logger.info("Could not find JSON attribute " + field + " : " + tweetJson);
                }
            }
        }

        return fieldData;
    }

    // define viral as favourite or retweet count as being over 100 times that of the user's follower count
    private static boolean viralTweet(String msg, Integer userFollowerCount )
    {
        for (String twitterField : viralTwitterFields) {
            Integer interactionCount = Integer.parseInt(extractFieldFromTweet(twitterField, msg));
            logger.info("field count " + twitterField + " : " + interactionCount + " vs follower count " + userFollowerCount);
            //if (interactionCount >= userFollowerCount*100 && interactionCount != 0) {
            if (interactionCount >= 0) {
                logger.info("VIRAL TWEET DETECTED");
                return true;
            }
        }

        return false;
    }

    private static void mapMonthValues()
    {
        monthsOfTheYear.put("Jan", 1);
        monthsOfTheYear.put("Feb", 2);
        monthsOfTheYear.put("Mar", 3);
        monthsOfTheYear.put("Apr", 4);
        monthsOfTheYear.put("May", 5);
        monthsOfTheYear.put("Jun", 6);
        monthsOfTheYear.put("Jul", 7);
        monthsOfTheYear.put("Aug", 8);
        monthsOfTheYear.put("Sep", 9);
        monthsOfTheYear.put("Oct", 10);
        monthsOfTheYear.put("Nov", 11);
        monthsOfTheYear.put("Dec", 12);
    }

    private static boolean checkIfTweetWithinOneDay(String tweetCreatedAtDate)
    {
        // EXAMPLE: date format of created_at field Mon Sep 09 2:43:58 +0000 2019
        // to know if the tweet was in the last day, only need year, day of month, 24 hr time
        if (tweetCreatedAtDate.isEmpty()) {
            return false;
        }

        String dateExploded[] = tweetCreatedAtDate.split(" ");
        Integer month = monthsOfTheYear.get(dateExploded[1]);
        Integer dayOfTheMonth = Integer.parseInt(dateExploded[2]);
        Integer hourOfDay = Integer.parseInt(dateExploded[3].split(":")[0]);
        Integer year = Integer.parseInt(dateExploded[5]);

        if (year < LocalDateTime.now().getYear()
                || month < LocalDateTime.now().getMonthValue()
                || (dayOfTheMonth < LocalDateTime.now().getDayOfMonth()-1)
                || (dayOfTheMonth < LocalDateTime.now().getDayOfMonth() && hourOfDay < LocalDateTime.now().getHour())
        ) {
            return false;
        }

        return true;
    }


    public void run()
    {
        logger.info("Setup");

        /** Set up blocking queues: be sure to size properly based on
         * expected TPS of stream
         */
        //tweets are sent to a message queue
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);

        // create a twitter client
        Client client = ProducerConfigs.createTwitterClient(msgQueue);

        //attempts to establish a connection
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = ProducerConfigs.createProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application and shutting down twitter client...");
            client.stop();
            logger.info("Closing producer...");
            producer.close();
            logger.info("Done!");
        }));

        // initialize hash maps for date values
        TwitterProducer.mapMonthValues();

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                String tweetCreatedDate = extractFieldFromTweet("created_at", msg);
                //logger.info(tweetCreatedDate);
                if (!checkIfTweetWithinOneDay(tweetCreatedDate)) {
                    continue;
                }
            } catch (InterruptedException | NullPointerException e) {
                logger.info("Exception found: " + msg);
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened, exception: ", e);
                        }
                    }
                });
            }

        }

        logger.info("End of application.");
    }
}

