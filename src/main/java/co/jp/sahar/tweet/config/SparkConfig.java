package co.jp.sahar.tweet.config;

import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String masterUri;


    @Value("${spring.data.cassandra.contactPoints}")
    private String contactPoints;

    @Value("${spring.data.cassandra.port}")
    private String port;


    @Bean
    public SparkConf conf() {
        return new SparkConf()
                .setAppName(appName)
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster(masterUri)
                .set("spark.executor.memory", "1g")
                .set("spark.cassandra.connection.host", contactPoints);
    }

    @Bean
    public JavaSparkContext sc() {
        return new JavaSparkContext(conf());
    }


    @Bean
    public CassandraConnector cassandraConnector() {


        return CassandraConnector.apply(conf());
    }


}
