package util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

public class CompleteDates {
    
    private final static int FIRST_YEAR = 2007; 
    private final static int FIRST_MONTH = 5; 

    private final static int LAST_YEAR = 2009; 
    private final static int LAST_MONTH = 9;
    
    public Map<String, Integer> writeDateValueMap() {
        Map<String, Integer> m = Maps.newTreeMap();
        for (int i = FIRST_YEAR; i <= LAST_YEAR; i++) {
            for (int j = 1; j <= 12; j++) {
                if(i == FIRST_YEAR && j >= FIRST_MONTH || (i == LAST_YEAR && j <= LAST_MONTH)  || (i != FIRST_YEAR && i!= LAST_YEAR)) {
                    m.put(i + ";" + String.format("%02d", j), 0);
                }
            }
        }
        return m;
    }
    
    public static void main(String[] args) throws IOException {
        //args 0 filename, args 1 hashtag
        final String fileName = args[0];
        final String hashtag = args[1];
        List<String> allLines = FileUtils.readLines(new File(fileName));
        
        Collection<String> tagLines = Collections2.filter(allLines, new Predicate<String>() {
            public boolean apply(String arg0) {
                return arg0.startsWith(hashtag);
            }
        });
        
        
        CompleteDates cd = new CompleteDates();
        Map<String, Integer> writeDateValueMap = cd.writeDateValueMap();
        
        for (String line : tagLines) {
            String[] split = StringUtils.split(line, "\t");
            String dateOnLine = split[0].replace(hashtag + ";", "");
            String countOnLine = split[1];
            writeDateValueMap.put(dateOnLine, Integer.valueOf(countOnLine));
        }
        
        //System.out.println(StringUtils.join(writeDateValueMap.entrySet(), "\n"));
        for (Entry<String, Integer> entry : writeDateValueMap.entrySet()) {
//            System.out.println(entry.getKey() + "\t" + entry.getValue());
            System.out.println(entry.getValue());
        }
    }

}
