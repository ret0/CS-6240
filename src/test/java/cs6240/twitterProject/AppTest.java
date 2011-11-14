package cs6240.twitterProject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

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
		testPatternMatch("i like #mj's songs more than #janet #jackson's!",
				"\\B+", Lists.newArrayList("i", "like", "#mj's", "songs",
						"more", "than", "#janet", "#jackson's"));
		testPatternMatch("i like #mj's songs more than #janet #jackson's!",
				"\\b\\w+\\b", Lists.newArrayList("i", "like", "#mj's", "songs",
						"more", "than", "#janet", "#jackson's"));

	}

	private void testPatternMatch(String testTweet, String testTag) {
		boolean matchResult = Predicates.containsPattern(
				"\\b" + testTag + "\\b").apply(testTweet);
		Assert.assertTrue(matchResult);
	}

	private void testPatternMatch(String testTweet, String testPattern,
			ArrayList<String> expectedWords) {

		ArrayList<String> actualWords = StringTools.split(testTweet,
				testPattern);

		for (String word : actualWords) {
			System.out.print(word + " ");
		}
		System.out.println();

		int i, j;
		for (i = 0, j = 0; i < actualWords.size() && j < expectedWords.size()
				&& actualWords.get(i).equals(expectedWords.get(i)); i++, j++)
			;

		Assert.assertTrue(i == actualWords.size() && j == expectedWords.size());
	}

	private void printResults(String line1, String line2) throws IOException {
		double d = App.fuzzyDistance(line1, line2);
		System.out.println("Line1: " + line1);
		System.out.println("Line2: " + line2);
		System.out.println("Score: " + d + "\n");
	}
}
