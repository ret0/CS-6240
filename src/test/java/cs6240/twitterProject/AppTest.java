package cs6240.twitterProject;

import java.io.IOException;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.commons.lang.StringEscapeUtils;
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
        Collection<String> actualWords = StringTools.splitEverything("i like #mj's\nsongs. more\tthan #janet      #jackson's! hash-tag #hash-tag -tag " +
                		"#-tag tag- #tag- #tag_as -aaa - aa# bb-  #123456 #1984");
        Assert.assertEquals(Lists.newArrayList("like", "#mj", "songs", "more", "than", "#janet", "#jackson", 
                                "hash-tag", "#hash-tag", "tag", "#-tag", "tag", "#tag", "#tag_as", "aaa", "aa", "bb", "#123456", "#1984"), actualWords);

        Collection<String> actualWords2 = StringTools.splitTagsOnly("i like #mj's\nsongs. more\tthan #janet      #JacKsoN's! hash-tag #Hash-tag -tag " +
                        "#-tag tag- #tag- #tag_as -aaa - aa# bb-  #123456 #1984");
        Assert.assertEquals(Lists.newArrayList("#mj", "#janet", "#jackson", 
                                "#hash-tag", "#-tag", "#tag", "#tag_as", "#123456", "#1984"), actualWords2);
        
        final byte[] string = "&#10025;".getBytes("UTF-8");
        //URLDecoder.
        String bla = StringEscapeUtils.unescapeHtml("A &#10025; Z");
        System.out.println("\u00c1");
//        System.out.println("\u10025");
//        System.out.println("\u2729");
        System.out.println(bla);

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
