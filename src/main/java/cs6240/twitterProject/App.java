package cs6240.twitterProject;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import util.MapSorter;

/**
 * Hello world!
 * 
 */
public class App {

	public static void main(String[] args) throws IOException {

		List<String> lines = FileUtils.readLines(new File(
				"data/SMSSpamCollectionHam.txt"));
		
		lines = lines.subList(0, 1000);

		Map<String[], Double> distances = new HashMap<String[], Double>();
		int counter = 0;
		for (String line1 : lines) {
		    System.out.println(counter++);
			for (String line2 : lines) {
			    
			    if(!line1.equals(line2)) {
			        double d = fuzzyDistance(line1, line2);
			        distances.put(new String[] { line1, line2 }, d);
			    }
				
			}
		}
		
		MapSorter<String[], Double> srt = new MapSorter<String[], Double>();
        Map<String[], Double> sortByValueTop = srt.sortByValue(distances);
        Map<String[], Double> sortByValueBottom = srt.sortByValue(distances, true);
        
        System.out.println("--------------------");
        System.out.println("TOP");
        System.out.println("--------------------");
        printPartialResults(sortByValueTop);
        System.out.println("--------------------");
        System.out.println("BOTTOM");
        System.out.println("--------------------");
        printPartialResults(sortByValueBottom);
	}

    private static void printPartialResults(Map<String[], Double> sortByValueTop) {
        int outcounter = 0;
        for (Entry<String[], Double> entry: sortByValueTop.entrySet()) {
            if (outcounter++ > 50) break;
            String line1E = entry.getKey()[0];
            String line2E = entry.getKey()[1];
            printResults(line1E, line2E, entry.getValue());
        }
    }

	public static double fuzzyDistance(String line1, String line2) throws IOException {

		List<String> sortedWords1 = Arrays.asList(StringUtils.split(line1.toLowerCase()));
		Collections.sort(sortedWords1);
		
		List<String> sortedWords2 = Arrays.asList(StringUtils.split(line2.toLowerCase()));
		Collections.sort(sortedWords2);
		
		List<String> sortedIntersection = ListUtils.intersection(sortedWords1, sortedWords2);
		Collections.sort(sortedIntersection);
		
		List<String> ignoredWords = FileUtils.readLines(new File("data/words"));
		sortedIntersection.removeAll(ignoredWords);

		String t0 = StringUtils.join(sortedIntersection, " ");
		
		String t1 = t0 + StringUtils.join(
				ListUtils.subtract(sortedWords1, sortedIntersection), " ");
		
		String t2 = t0 + StringUtils.join(
				ListUtils.subtract(sortedWords2, sortedIntersection), " ");

//		int t2 = EditDistance.getLevenshteinDistance(sortedIntersectionStr
//				+ restOfLine1, sortedIntersectionStr + restOfLine2);
//		int t1 = EditDistance.getLevenshteinDistance(sortedIntersectionStr,
//				sortedIntersectionStr + restOfLine2);
//		int t0 = EditDistance.getLevenshteinDistance(sortedIntersectionStr
//				+ restOfLine1, sortedIntersectionStr);
		
		

		//return Math.min(Math.min(t0, t1), t2);
		//return 0.9 * t0 + 0.05 * t1 + 0.05 * t2;
//		final int t0t1 = EditDistance.getLevenshteinDistance(t0, t1);
//		final int t0t2 = EditDistance.getLevenshteinDistance(t0, t2);
//		final int t1t2 = EditDistance.getLevenshteinDistance(t1, t2);
        //return Math.min(Math.min(t0t1, t0t2), t1t2);
		//return 0.5 * t0t1 + 0.5* t0t2;
		return ((((double)sortedIntersection.size()) / t1.length()) * 
		        (((double)sortedIntersection.size()) / t2.length())) * 100;
	}
	
	 private static void printResults(String line1, String line2, Double d) {
	        System.out.println("Line1: " + line1);
	        System.out.println("Line2: " + line2);
	        System.out.println("Score: " + d + "\n");
	    }
}
