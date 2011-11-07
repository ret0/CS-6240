package cs6240.twitterProject;

import java.io.IOException;

import org.junit.Test;



public class AppTest {

    @Test
    public void testStringMatching() throws IOException
    {
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

    private void printResults(String line1, String line2) throws IOException {
        double d = App.fuzzyDistance(line1, line2);
        System.out.println("Line1: " + line1);
        System.out.println("Line2: " + line2);
    	System.out.println("Score: " + d + "\n");
    }
}
