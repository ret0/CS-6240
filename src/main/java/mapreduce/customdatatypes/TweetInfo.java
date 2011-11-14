package mapreduce.customdatatypes;

import java.util.List;

import util.StringTools;



public class TweetInfo {

    private final String completeLineFromFile;
    private final String tweetContent;

    public TweetInfo(String completeLine) {
        this.completeLineFromFile = completeLine;
        this.tweetContent = readTweetContent();
    }

    private String readTweetContent() {
        String[] words = this.completeLineFromFile.toString().split("\t");
        return words[words.length - 1];
    }
    
    public List<String> getAllWords() {
        return StringTools.splitEverything(tweetContent);
    }
    
    public List<String> getAllHashtags() {
        return StringTools.splitTagsOnly(tweetContent);
    }

    public String toString() {
        return completeLineFromFile;
    }
    
}
