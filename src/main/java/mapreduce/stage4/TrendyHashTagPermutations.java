package mapreduce.stage4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrendyHashTagPermutations extends Configured implements Tool {

	// universal constants
	public static final String VARNAME_TRENDY_HASHTAGS_LIST = "TRENDY_HASHTAGS_LIST";
	public static final String VARNAME_STOPWORDS_LIST = "STOPWORDS_LIST";

	// TRENDY_HASHTAGS_LIST: "/user/venugopalan.s/stage1Out/trendyHashtags.txt"
	// STOPWORDS_LIST: "/user/venugopalan.s/stopwords.txt"

	public static class Reduce extends MapReduceBase implements
			Reducer<TextWritable, IntWritable, Text, IntWritable> {

        public void reduce(TextWritable key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int currentPermutationCount = 0;
			while (values.hasNext()) {
			    currentPermutationCount += values.next().get();
			}
			String hashA = ((Text) key.get()[0]).toString();
			int hashACount = Integer.valueOf(((Text) key.get()[1]).toString());
			String hashB = ((Text) key.get()[2]).toString();
			int hashBCount = Integer.valueOf(((Text) key.get()[3]).toString());
			double score = (currentPermutationCount / (hashACount + hashBCount)) * 100; //FIXME; 
			final IntWritable tagPairScore = new IntWritable((int)score);
            output.collect(new Text(hashA + "," + hashB), tagPairScore);
		}

	}

	static int printUsage() {
		System.out.println("stage4 wrong args");
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
		JobConf conf = new JobConf(getConf(),
				TrendyHashTagPermutations.class);
		conf.setJobName("stage4");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapOutputKeyClass(TextWritable.class);
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
		if (other_args.size() != 3) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 3.");
			return printUsage();
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(2)));

		conf.set(VARNAME_TRENDY_HASHTAGS_LIST, other_args.get(1));
		DistributedCache.addCacheFile(new Path(other_args.get(1)).toUri(), conf);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new TrendyHashTagPermutations(), args);
		System.exit(res);
	}
}
