package cs6240.twitterProject;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import javax.swing.text.html.MinimalHTMLWriter;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Hello world!
 * 
 */
public class App {

	public static void main(String[] args) throws IOException {

		List<String> lines = FileUtils.readLines(new File(
				"data/SMSSpamCollection"));

		HashMap<String[], Double> distances = new HashMap<String[], Double>();
		
		for (String line1 : lines) {
			for (String line2 : lines) {
				
				double d = fuzzyDistance(line1, line2);
				distances.put(new String[] { line1, line2 }, d);

				if (d < 30) {
					System.out.println(line1);
					System.out.println(line2);
					System.out.println(d);
				}
			}
		}
	}

	public static double fuzzyDistance(String line1, String line2) {

		List<String> words1 = Arrays.asList(StringUtils.split(line1.toLowerCase()));
		Collections.sort(words1);
		List<String> words2 = Arrays.asList(StringUtils.split(line2.toLowerCase()));
		Collections.sort(words2);
		
		List<String> sortedIntersection = ListUtils
				.intersection(words1, words2);
		Collections.sort(sortedIntersection);

		String sortedIntersectionStr = StringUtils
				.join(sortedIntersection, " ");
		String restOfLine1 = StringUtils.join(
				ListUtils.subtract(words1, sortedIntersection), " ");
		String restOfLine2 = StringUtils.join(
				ListUtils.subtract(words2, sortedIntersection), " ");

		int t0 = EditDistance.getLevenshteinDistance(sortedIntersectionStr
				+ restOfLine1, sortedIntersectionStr + restOfLine2);
		int t1 = EditDistance.getLevenshteinDistance(sortedIntersectionStr,
				sortedIntersectionStr + restOfLine2);
		int t2 = EditDistance.getLevenshteinDistance(sortedIntersectionStr
				+ restOfLine1, sortedIntersectionStr);

		//return Math.min(Math.min(t0, t1), t2);
		return 0.8 * t0 + 0.1 * t1 + 0.1 * t2;
	}
}
