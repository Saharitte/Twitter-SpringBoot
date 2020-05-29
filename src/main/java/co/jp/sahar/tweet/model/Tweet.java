package co.jp.sahar.tweet.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;


@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Tweet implements Serializable {

    private static final long serialVersionUID = 1L;


    private Long id;

    private String text;

    private String language;

    private Date createat;

    private Set<String> hashtags;


}
