package util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

/**
 * Regular expressions to extract specific elements from tweets 
 */
public class StringTools {

	private static final String DEFAULT_TWITTER_WORDS_AND_TAGS = "(#|\\w)(\\-|\\w)*\\w";
	private static final Pattern DEFAULT_PATTERN = Pattern.compile(DEFAULT_TWITTER_WORDS_AND_TAGS);

	private static final String DEFAULT_TWITTER_TAGS_ONLY = "#(\\-|\\w)*\\w";
	private static final Pattern PATTERN_TAGS_ONLY = Pattern.compile(DEFAULT_TWITTER_TAGS_ONLY);
	
	private static final String DEFAULT_USERNAMES_ONLY = "@(\\w)+(:?)";
	private static final Pattern PATTERN_USERNAMES_ONLY = Pattern.compile(DEFAULT_USERNAMES_ONLY);
	
	// hashtags with hyphens?
    // http://erictarn.com/post/1060722347/the-best-twitter-hashtag-regular-expression

    public static List<String> splitEverything(String content) {
		return split(content, DEFAULT_PATTERN);
	}

    public static List<String> splitTagsOnly(String content) {
        return split(content, PATTERN_TAGS_ONLY);
    }

    public static List<String> splitUsernamesOnly(String content) {
        return split(content, PATTERN_USERNAMES_ONLY);
    }
    
	private static List<String> split(String content, Pattern regex) {
		List<String> words = Lists.newArrayList();
		Matcher matcher = regex.matcher(content.toLowerCase());
		while (matcher.find()) {
			words.add(matcher.group());
		}
		return words;
	}
}
