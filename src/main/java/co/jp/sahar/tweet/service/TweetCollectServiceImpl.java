package co.jp.sahar.tweet.service;

import co.jp.sahar.tweet.model.Tweet;
import co.jp.sahar.tweet.utils.exceptions.KeywordNotFoundException;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import twitter4j.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class TweetCollectServiceImpl implements TweetCollectService, Serializable {


    @Autowired
    transient JavaSparkContext sc;


    @Autowired
    CassandraConnector cassandraConnector;


    @Value("${spring.data.cassandra.keyspace}")
    String keyspace;

    @Value("${spring.data.cassandra.table}")
    String table;


    @Value("${twitter.api.consumer.key}")
    String consumerKey;

    @Value("${twitter.api.consumer.secret}")
    String consumerSecret;

    @Value("${twitter.access.token}")
    String accessToken;
    @Value("${twitter.access.secret}")
    String accessTokenSecret;


    private void saveToDB(JavaDStream<Tweet> tweets, String[] keywords) {

        tweets.foreachRDD(rdd -> {

            CassandraJavaUtil.javaFunctions(rdd)

                    .writerBuilder(keyspace, table, CassandraJavaUtil.mapToRow(Tweet.class)).saveToCassandra();

        });


    }


    private void createCassandraTable(String lang) {


        try (Session session = cassandraConnector.openSession()) {

            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS  " + keyspace + "." + table + " (" +
                    "id bigint PRIMARY KEY,\n" +
                    "text text,\n" +
                    "language text,\n" +

                    "hashtags set<text>,\n" +

                    "createat date)");


        }


    }


    private Set<String> getHashtags(String text) {


        Pattern MY_PATTERN = Pattern.compile("#(\\S+)");
        Matcher mat = MY_PATTERN.matcher(text);
        Set<String> hashtags = new HashSet<>();
        while (mat.find()) {
            hashtags.add(mat.group(1));
        }

        return hashtags;
    }

    @Override
    public void getTweets(String[] filters) throws InterruptedException {

        setTwitterConfig();

        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(2000));


        JavaDStream<Status> stream = TwitterUtils.createStream(jssc, filters);

        stream.window(Durations.seconds(600000));

        JavaDStream<Tweet> statuses = stream.map(
                (Function<Status, Tweet>) status -> new Tweet(status.getId(), status.getText(), status.getLang(), status.getCreatedAt(), getHashtags(status.getText()))
        );


        //create cassandra tables
        createCassandraTable("en");

        //save english tweet to cassandra
        saveToDB(statuses.filter(st -> st.getLanguage().equals("en")), filters);


        jssc.start();

        jssc.awaitTermination();


    }


    @Override
    public List<String> searchTweets(String topic) {

        setTwitterConfig();

        Twitter twitter = new TwitterFactory().getInstance();

        ArrayList<String> tweetList = new ArrayList<>();

        try {

            Query query = new Query(topic);

            QueryResult result;

            do {

                result = twitter.search(query);

                List<Status> tweets = result.getTweets();

                for (Status tweet : tweets) {

                    tweetList.add(tweet.getText());

                }

            } while ((query = result.nextQuery()) != null);

        } catch (TwitterException te) {

            te.printStackTrace();

            System.out.println("Failed to search tweets: " + te.getMessage());

        }
        return tweetList;
    }

    @Override
    public List<Tweet> loadTweetsFromDB() throws KeywordNotFoundException {

        JavaRDD<Tweet> tweetRDD = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable(keyspace, table, CassandraJavaUtil.mapRowTo(Tweet.class));

        return tweetRDD.collect();

    }


    private void setTwitterConfig() {

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

    }


}

