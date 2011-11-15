package mapreduce.stage4;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextWritable extends ArrayWritable {
    
    public TextWritable() {
        super(Text.class); 
    }
}
