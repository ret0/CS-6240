package cs6240.twitterProject;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }


    public void testApp()
    {
    	String line1 = "Free passes to the Rangers tournament!";
    	String line2 = "rangers ftw hahahahaaaaaaaaaaaaaa :)";
    	double d = App.fuzzyDistance(line1, line2);
    	System.out.println(d);
        assertTrue(d < 30);
        
        
    	line1 = "Free passes to the Rangers tournament!";
    	line2 = "rangers ftw";
    	d = App.fuzzyDistance(line1, line2);
    	System.out.println(d);
        assertTrue(d < 30);

        
    	line1 = "Free passes to the Rangers tournament!";
    	line2 = "rangers";
    	d = App.fuzzyDistance(line1, line2);
    	System.out.println(d);
        assertTrue(d < 30);
        
        
    	line1 = "Free passes to the Rangers tournament!";
    	line2 = "rangers passes";
    	d = App.fuzzyDistance(line1, line2);
    	System.out.println(d);
        assertTrue(d < 30);
        
    	line1 = "Free passes to the Yankees tournament!";
    	line2 = "rangers passes";
    	d = App.fuzzyDistance(line1, line2);
    	System.out.println(d);
        assertTrue(d < 30);
        
       	line1 = "Free passes to the Yankees tournament!";
    	line2 = "rangers eat pizza";
    	d = App.fuzzyDistance(line1, line2);
    	System.out.println(d);
        assertTrue(d < 30);
 
    }
}
