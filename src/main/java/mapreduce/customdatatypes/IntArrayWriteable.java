package mapreduce.customdatatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWriteable extends ArrayWritable {

	public IntArrayWriteable() {
		super(IntWritable.class);
	}
	
	public IntArrayWriteable(Writable[] values) {
		super(IntWritable.class, values);
	}
	
}
