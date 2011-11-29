package mapreduce.customdatatypes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import util.StringTools;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;


public class TweetInfo {
	
	/*
	 * 0 [tweetid]  
	 * 1 [userid] 
	 * 2 [timestamp] 
	 * 3 [reply-tweetid] 
	 * 4 [reply-userid] 
	 * 5 [source] 
	 * 6 [truncated?] 
	 * 7 [favorited?] 
	 * 8 [location] 
	 * 9 [text]
	 */
	
    private static final String ELEMENT_SEPARATOR = "\t";
	private static final int USERID_INDEX = 1;
    private static final int DATE_INDEX = 2;

    private final static SimpleDateFormat TWITTER_DATE_FORMAT = 
            new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");

    private static final Set<String> STOP_WORDS = Sets.newHashSet("the", "be",
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
    
    private final String tweetContent;
    private final String[] lineElements;
    private final DateTime tweetDateTime; 

    public TweetInfo(String completeLine) {
        lineElements = completeLine.split(ELEMENT_SEPARATOR);
        tweetContent = readTweetContent();
        tweetDateTime = readTweetTimeStamp();
    }

    private DateTime readTweetTimeStamp() {
        try {
            return new DateTime(TWITTER_DATE_FORMAT.parse(lineElements[DATE_INDEX]));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private String readTweetContent() {
        final int contentIndex = lineElements.length - 1;
        return StringEscapeUtils.unescapeHtml(lineElements[contentIndex]);
    }
    
    /**
     * @return all words (including hashtags)
     */
    public List<String> getAllWords() {
        return StringTools.splitEverything(tweetContent);
    }

    public Collection<String> getAllMeaningfulWords() {
        return  Collections2.filter(getAllWords(), new Predicate<String>() {
			@Override
			public boolean apply(String arg0) {
				return !STOP_WORDS.contains(arg0);
			}
		}) ;
    }
    
    public List<String> getAllHashtags() {
        return StringTools.splitTagsOnly(tweetContent);
    }

    public List<String> getAllUsernames() {
        return StringTools.splitUsernamesOnly(tweetContent);
    }

    public Set<String> getTrends(Set<String> trendyTags){
    	Set<String> tags = Sets.newHashSet(this.getAllHashtags());
    	return Sets.intersection(tags, trendyTags);
    }
    
    public boolean isRetweet(){
    	return this.tweetContent.trim().startsWith("RT @");
    }
    
    public DateTime getTweetDateTime() {
        return tweetDateTime;
    }

    public String toString() {
        return StringUtils.join(lineElements, " ");
    }

	public long getAuthorId() {
		return Long.valueOf(this.lineElements[USERID_INDEX]); 
		
	}
    
}
