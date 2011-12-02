package mapreduce.customdatatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Typed custom type that is required to be able to work with ArrayWritables
 * See hadoop doc http://hadoop.apache.org/common/docs/r0.20.2/api/org/apache/hadoop/io/ArrayWritable.html
 */
public class IntArrayWriteable extends ArrayWritable {

	public IntArrayWriteable() {
		super(IntWritable.class);
	}
	
	public IntArrayWriteable(Writable[] values) {
		super(IntWritable.class, values);
	}
	
}
