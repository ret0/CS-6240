package util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateMidnight;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

public class CompleteDays {
    
    private final static DateMidnight FIRST_DATE = new DateMidnight(2008, 10, 1);
    private final static DateMidnight LAST_DATE =new DateMidnight(2009, 9, 1);
    
    public Map<String, Integer> writeDateValueMap() {
        Map<String, Integer> m = Maps.newTreeMap();
        DateMidnight currentDate = FIRST_DATE;
        
        while(currentDate.isBefore(LAST_DATE)) {
            final int year = currentDate.getYear();
            final String month = String.format("%02d", currentDate.getMonthOfYear());
            final String day = String.format("%02d", currentDate.getDayOfMonth());
            m.put(year + "-" + month + "-" + day, 0);
            currentDate = currentDate.plusDays(1);
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
        
        
        CompleteDays cd = new CompleteDays();
        Map<String, Integer> writeDateValueMap = cd.writeDateValueMap();
       
        for (String line : tagLines) {
            String[] split = StringUtils.split(line, "\t");
            String dateOnLine = split[0].replace(hashtag + ";", "");
            String countOnLine = split[1];
            writeDateValueMap.put(dateOnLine, Integer.valueOf(countOnLine));
        }
        
//        System.out.println(StringUtils.join(writeDateValueMap.entrySet(), "\n"));
        for (Entry<String, Integer> entry : writeDateValueMap.entrySet()) {
//            System.out.println(entry.getKey() + "\t" + entry.getValue());
            System.out.println(entry.getValue());
        }
    }

}
