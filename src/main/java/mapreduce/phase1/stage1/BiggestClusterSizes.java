package mapreduce.phase1.stage1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mapreduce.customdatatypes.TweetInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Phase 1, Stage 1: Count all Hashtag occurences
 */
public class BiggestClusterSizes extends Configured implements Tool {

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

		    TweetInfo tweetInfo = new TweetInfo(value.toString());

			for (String tag : tweetInfo.getAllHashtags()) {
			    output.collect(new Text(tag), ONE);
            }
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, IntWritable, Text> {

        private static final int MIN_TAG_OCCURANCES = 1000;

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if (sum > MIN_TAG_OCCURANCES)
				output.collect(new IntWritable(sum), key);
		}
	}

	static int printUsage() {
		System.out.println("stage1 [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for word count map/reduce program. Invoke this method to
	 * submit the map/reduce job.
	 * 
	 * @throws IOException
	 *             when there are communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), BiggestClusterSizes.class);
		conf.setJobName("stage1");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			return printUsage();
		}
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new BiggestClusterSizes(), args);
		System.exit(res);
	}
}
