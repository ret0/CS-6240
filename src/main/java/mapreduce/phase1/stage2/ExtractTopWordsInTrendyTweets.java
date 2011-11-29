package mapreduce.phase1.stage2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

import com.google.common.collect.Maps;

public class ExtractTopWordsInTrendyTweets extends Configured implements Tool {

	// universal constants
	public static final String VARNAME_TRENDY_HASHTAGS_LIST = "TRENDY_HASHTAGS_LIST";
	public static final String VARNAME_STOPWORDS_LIST = "STOPWORDS_LIST";

	// TRENDY_HASHTAGS_LIST: "/user/venugopalan.s/stage1Out/trendyHashtags.txt"
	// STOPWORDS_LIST: "/user/venugopalan.s/stopwords.txt"

	/**
	 * REDUCER:
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, MapWritable, Text, Text> {

		private static final int TOP_N_WORDS_LIMIT = 100;

		public void reduce(Text key, Iterator<MapWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// create result wordcount stripe
			TreeMap<String, Integer> mergedWordCounts = Maps.newTreeMap();
			// sum over partial wordcount stripes
			MapWritable mw;
			while (values.hasNext()) {
				mw = values.next();

				for (Entry<Writable, Writable> e : mw.entrySet()) {
					String word = ((Text) e.getKey()).toString();
					int count = ((IntWritable) e.getValue()).get();
					if (mergedWordCounts.containsKey(word))
						mergedWordCounts.put(word, count + 1);
					else
						mergedWordCounts.put(word, count);
				}
			}

			// get top N entries
			Map<String, Integer> topNWordCounts = Maps.newHashMap();
			for (int i = 0; i < TOP_N_WORDS_LIMIT; i++) {
				int maxVal = -1;
				String maxKey = "";
				for (Entry<String, Integer> e : mergedWordCounts.entrySet()) {
					if (e.getValue() > maxVal) {
						maxVal = e.getValue();
						maxKey = e.getKey();
					}
				}
				mergedWordCounts.remove(maxKey);
				topNWordCounts.put(maxKey, maxVal);
			}
			mergedWordCounts = null;

			output.collect(key, new Text(StringUtils.join(topNWordCounts.keySet(), ",")));
		}

	}

	static int printUsage() {
		System.out
				.println("stage2 [-m <maps>] [-r <reduces>] <input> <output>");
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
				ExtractTopWordsInTrendyTweets.class);
		conf.setJobName("stage2");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(MapWritable.class);

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
		DistributedCache
				.addCacheFile(new Path(other_args.get(1)).toUri(), conf);
		
		/*
		conf.set(VARNAME_STOPWORDS_LIST, other_args.get(2));
		DistributedCache
				.addCacheFile(new Path(other_args.get(2)).toUri(), conf);
		*/
		
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new ExtractTopWordsInTrendyTweets(), args);
		System.exit(res);
	}
}
