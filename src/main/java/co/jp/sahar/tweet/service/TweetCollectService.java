package co.jp.sahar.tweet.service;

import co.jp.sahar.tweet.model.Tweet;

import java.util.List;


public interface TweetCollectService {


    void getTweets(String[] filters) throws InterruptedException;

    List<String> searchTweets(String topic);

    List<Tweet> loadTweetsFromDB();
}
