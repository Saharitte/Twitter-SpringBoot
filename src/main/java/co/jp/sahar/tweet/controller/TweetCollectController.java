package co.jp.sahar.tweet.controller;


import co.jp.sahar.tweet.model.Tweet;
import co.jp.sahar.tweet.service.TweetCollectService;
import co.jp.sahar.tweet.utils.exceptions.KeywordNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TweetCollectController {

    @Autowired
    TweetCollectService service;

    @PostMapping(path = "/loadTweetsFromDB")
    public List<Tweet> loadTweetsFromDB() throws KeywordNotFoundException {

        return service.loadTweetsFromDB();

    }


    @PostMapping(path = "/stream")
    public void stream(@RequestParam("keywords") String filters) throws InterruptedException {

        service.getTweets(filters.split(","));
    }

    @PostMapping(path = "/searchTweets")
    public List<String> searchTweets(@RequestParam("topic") String topic) {

        return service.searchTweets(topic);

    }

}
