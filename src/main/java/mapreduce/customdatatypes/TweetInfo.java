package mapreduce.customdatatypes;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;

import util.StringTools;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;



public class TweetInfo {

    private final String completeLineFromFile;
    private final String tweetContent;

	private static final Set<String> stopWords = Sets.newHashSet("the", "be",
			"to", "of", "and", "a", "in", "that", "have", "i", "it", "for",
			"not", "on", "with", "he", "as", "you", "do", "at", "this", "but",
			"his", "by", "from", "they", "we", "say", "her", "she", "or", "an",
			"will", "my", "one", "all", "would", "there", "their", "what",
			"so", "up", "out", "if", "about", "who", "get", "which", "go",
			"me", "when", "make", "can", "like", "time", "no", "just", "him",
			"know", "take", "person", "into", "year", "your", "good", "some",
			"could", "them", "see", "other", "than", "then", "now", "look",
			"only", "come", "its", "over", "think", "also", "back", "after",
			"use", "two", "how", "our", "work", "first", "well", "way");
	
    public TweetInfo(String completeLine) {
        this.completeLineFromFile = completeLine;
        this.tweetContent = readTweetContent();
    }

    private String readTweetContent() {
        String[] words = this.completeLineFromFile.toString().split("\t");
        return StringEscapeUtils.unescapeHtml(words[words.length - 1]);
    }
    
    public List<String> getAllWords() {
        return StringTools.splitEverything(tweetContent);
    }

    public Collection<String> getAllMeaningfulWords() {
        return  Collections2.filter(getAllWords(), new Predicate<String>() {
			@Override
			public boolean apply(String arg0) {
				return !stopWords.contains(arg0);
			}
		}) ;
    }
    
    public List<String> getAllHashtags() {
        return StringTools.splitTagsOnly(tweetContent);
    }

    public String toString() {
        return completeLineFromFile;
    }
    
}
