package cs6240.twitterProject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import junit.framework.Assert;
import mapreduce.customdatatypes.TweetInfo;
import mapreduce.phase2.stage1.pigudfs.FIND_MONTHLY_SIZE;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;
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
    public void testFIND_MONTHLY_SIZE() throws IOException {

		try {
    	Tuple t = TupleFactory.getInstance().newTuple(9);
    	t.set(0, 2109);                                                                                                                                
    	t.set(1, 14);                                                                                                                                                   
    	t.set(2, "Fri Apr 14 04:48:47 +0000 2006");                                                                                                                       
    	t.set(3, "web");                                                                                                                          
    	t.set(4, "web");                                                                                                                                                  
    	t.set(5, false);                                                                                                                                                
    	t.set(6, false);                                                                                                                                                
    	t.set(7, "San Francisco");                                                                                                                                        
    	t.set(8, "leaving the office #abc soon. Thinking that making butterflies for others is very nice. Tonight I sleep with japanese pandas in a dark bamboo forest.");
   
    	
    	DataBag actualBag = new FIND_MONTHLY_SIZE().exec(t);
    	DataBag expectedBag = BagFactory.getInstance().newDefaultBag();
    	
    	Tuple result = TupleFactory.getInstance().newTuple(4);
		result.set(0, "#abc");

		Date date = null;
			date = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
					.parse((String) t.get(2));
		
		DateTime jodaDate = new DateTime(date);
		result.set(1, jodaDate.getMonthOfYear());
		result.set(2, jodaDate.getYear());
		
		result.set(3, 1);

		expectedBag.add(result);
		
    	Assert.assertEquals(actualBag, expectedBag);
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("bad bad" + e.getMessage());
			e.printStackTrace();
		}

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

		// /TODO: differentiate the elements in a tweet: hashtags, users, URLs
		// (bit.ly or others), email addresses, sitenames e.g. google.com,
		// years, zipcodes , the most commonly used unicode charactes on
		// twitter, smileys

		// /PROBLEMS: html & utf8 encodings, large numbers with commas in betw,

		Collection<String> actualWords = StringTools
				.splitEverything("i like #mj's\nsongs. more\tthan #janet @sapna      #jackson's! hash-tag #hash-tag -tag "
						+ "#-tag tag- #tag- #tag_as -aaa - aa# bb-  #123456 #1984");
		Assert.assertEquals(Lists.newArrayList("like", "#mj", "songs", "more",
				"than", "#janet", "sapna", "#jackson", "hash-tag", "#hash-tag",
				"tag", "#-tag", "tag", "#tag", "#tag_as", "aaa", "aa", "bb",
				"#123456", "#1984"), actualWords);

		Collection<String> actualWords2 = StringTools
				.splitTagsOnly("i like #mj's\nsongs. more\tthan #janet      #JacKsoN's! hash-tag #Hash-tag -tag "
						+ "#-tag tag- #tag- #tag_as -aaa - aa# bb-  #123456 #1984");
		Assert.assertEquals(Lists.newArrayList("#mj", "#janet", "#jackson",
				"#hash-tag", "#-tag", "#tag", "#tag_as", "#123456", "#1984"),
				actualWords2);

		Collection<String> actualWords3 = new TweetInfo(
				"&lt;hi&quot; #&#193;-tag").getAllWords();
		Assert.assertEquals(Lists.newArrayList("hi", "tag"), actualWords3);

		Collection<String> actualWords4 = new TweetInfo(
				"DO YOU COPY&#193;&#194; people?").getAllWords();
		Assert.assertEquals(Lists.newArrayList("do", "you", "copy", "people"),
				actualWords4); // "\u00C1", "\u00C2",
	}

	private void testPatternMatch(String testTweet, String testTag) {
		boolean matchResult = Predicates.containsPattern(
				"\\b" + testTag + "\\b").apply(testTweet);
		Assert.assertTrue(matchResult);
	}

	@Test
	public void testDateParsing() throws ParseException {
		Date date = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
				.parse("Tue Mar 21 21:00:54 +0000 2006");
		System.out.println(new DateTime(date));
		// DateTimeFormatter forPattern = DateTimeFormat.forPattern(");
		// DateTime dt = forPattern.parseDateTime();
		// System.out.println(dt);
	}

	private void printResults(String line1, String line2) throws IOException {
		// double d = App.fuzzyDistance(line1, line2);
		// System.out.println("Line1: " + line1);
		// System.out.println("Line2: " + line2);
		// System.out.println("Score: " + d + "\n");
	}
}
