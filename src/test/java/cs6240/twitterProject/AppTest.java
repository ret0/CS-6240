package cs6240.twitterProject;

import java.io.IOException;
import java.util.Collection;

import junit.framework.Assert;
import mapreduce.customdatatypes.TweetInfo;

import org.junit.Test;

import util.StringTools;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

public class AppTest {

    @Test
    public void testStringMatching() throws IOException {
        String line1 = "Free passes to the Rangers tournament!";
        String line2 = "rangers ftw hahahahaaaaaaaaaaaaaa :)";
        printResults(line1, line2);

        line1 = "Free passes to the Rangers tournament!";
        line2 = "rangers ftw";
        printResults(line1, line2);

        line1 = "Free passes to the Rangers tournament!";
        line2 = "rangers";
        printResults(line1, line2);

        line1 = "Free passes to the Rangers tournament!";
        line2 = "rangers passes";
        printResults(line1, line2);

        line1 = "Free passes to the Yankees tournament!";
        line2 = "rangers passes";
        printResults(line1, line2);

        line1 = "Free passes to the Yankees tournament!";
        line2 = "rangers eat pizza";
        printResults(line1, line2);
    }

    @Test
    public void testGuavaContains() throws IOException {
        testPatternMatch("A Tweet.", "Tweet");
        testPatternMatch("A Tweet?", "Tweet");
        testPatternMatch("A Tweet,", "Tweet");
        testPatternMatch("A ?Tweet,.", "Tweet");
        testPatternMatch("A \"Tweet\"", "Tweet");
        testPatternMatch("A -Tweet-", "Tweet");
    }

    @Test
    public void testStringSplit() throws IOException {
        // testPatternMatch("i like #mj's songs more than #janet #jackson's!",
        // "\\B+", Lists.newArrayList("i", "like", "#mj's", "songs",
        // "more", "than", "#janet", "#jackson's"));

        
        // testPatternMatch("i like #mj's songs. more than #janet #jackson's!",
        // "\\b(#?\\w+)\\b", Lists.newArrayList("i", "like", "#mj", "s",
        // "songs",
        // "more", "than", "#janet", "#jackson", "s"));
        
        // # is considered a non word char, therefore it's a boundary
        // Tested using http://gskinner.com/RegExr/
    	
    	///TODO: differentiate the elements in a tweet: hashtags, users, URLs (bit.ly or others), email addresses, sitenames e.g. google.com, years, zipcodes , the most commonly used unicode charactes on twitter, smileys
    	
    	///PROBLEMS: html & utf8 encodings, large numbers with commas in betw, 
    	
        Collection<String> actualWords = StringTools.splitEverything("i like #mj's\nsongs. more\tthan #janet @sapna      #jackson's! hash-tag #hash-tag -tag " +
                		"#-tag tag- #tag- #tag_as -aaa - aa# bb-  #123456 #1984");
        Assert.assertEquals(Lists.newArrayList("like", "#mj", "songs", "more", "than", "#janet", "sapna", "#jackson", 
                                "hash-tag", "#hash-tag", "tag", "#-tag", "tag", "#tag", "#tag_as", "aaa", "aa", "bb", "#123456", "#1984"), actualWords);

        Collection<String> actualWords2 = StringTools.splitTagsOnly("i like #mj's\nsongs. more\tthan #janet      #JacKsoN's! hash-tag #Hash-tag -tag " +
                        "#-tag tag- #tag- #tag_as -aaa - aa# bb-  #123456 #1984");
        Assert.assertEquals(Lists.newArrayList("#mj", "#janet", "#jackson", 
                                "#hash-tag", "#-tag", "#tag", "#tag_as", "#123456", "#1984"), actualWords2);
        
        
        Collection<String> actualWords3 = new TweetInfo("&lt;hi&quot; #&#193;-tag").getAllWords();
        Assert.assertEquals(Lists.newArrayList("hi", "tag"), actualWords3);

        
        Collection<String> actualWords4 = new TweetInfo("DO YOU COPY&#193;&#194; people?").getAllWords();
        Assert.assertEquals(Lists.newArrayList("do", "you", "copy", "people"), actualWords4); // "\u00C1", "\u00C2", 
    }

    private void testPatternMatch(String testTweet, String testTag) {
        boolean matchResult = Predicates.containsPattern(
                "\\b" + testTag + "\\b").apply(testTweet);
        Assert.assertTrue(matchResult);
    }

    private void printResults(String line1, String line2) throws IOException {
        //double d = App.fuzzyDistance(line1, line2);
//        System.out.println("Line1: " + line1);
//        System.out.println("Line2: " + line2);
//        System.out.println("Score: " + d + "\n");
    }
}
