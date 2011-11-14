package mapreduce.stage2;

import java.awt.datatransfer.StringSelection;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mapreduce.customdatatypes.StringSet;
import mapreduce.customdatatypes.TweetInfo;
import mapreduce.customdatatypes.WordCountDescComparator;
import mapreduce.customdatatypes.WordCountMap;
import mapreduce.customdatatypes.WordCountStripeFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ExtractTopWordsInTrendyTweets extends Configured implements Tool {

	// universal constants
	public static final String VARNAME_TRENDY_HASHTAGS_LIST = "TRENDY_HASHTAGS_LIST";
	public static final String VARNAME_STOPWORDS_LIST = "STOPWORDS_LIST";

	// TRENDY_HASHTAGS_LIST: "/user/venugopalan.s/stage1Out/trendyHashtags.txt"
	// STOPWORDS_LIST: "/user/venugopalan.s/stopwords.txt"

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, MapWritable> {

		// constructed in each map task
		private final HashMap<String, Integer> trendyHashtags = new HashMap<String, Integer>();
		// private static final HashSet<String> stopwords = new
		// HashSet<String>();

		// constants across map tasks
		private static final IntWritable one = new IntWritable(1);
	
		// hashtags with hyphens?
		// http://erictarn.com/post/1060722347/the-best-twitter-hashtag-regular-expression

		public void configure(JobConf conf) {
			try {
				String trendyHashtagsCacheName = new Path(
						conf.get(VARNAME_TRENDY_HASHTAGS_LIST)).getName();

				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
				if (null != cacheFiles && cacheFiles.length > 0) {
					for (Path cachePath : cacheFiles) {
						if (cachePath.getName().equals(trendyHashtagsCacheName)) {
							loadTrendyHashtags(cachePath);
							break;
						}
					}
				}
			} catch (IOException ioe) {
				System.err
						.println("IOException reading from distributed cache");
				System.err.println(ioe.toString());
			}
		}

		private void loadTrendyHashtags(Path cachePath) throws IOException {
			BufferedReader lineReader = new BufferedReader(new FileReader(
					cachePath.toString()));
			try {
				String line;
				while ((line = lineReader.readLine()) != null) {
					String[] words = line.split("\t");
					this.trendyHashtags.put(words[1],
							Integer.parseInt(words[0]));
				}
			} finally {
				lineReader.close();
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, MapWritable> output, Reporter reporter)
				throws IOException {
			/*
			 * 0 [tweetid] 1 [userid] 2 [timestamp] 3 [reply-tweetid] 4
			 * [reply-userid] 5 [source] 6 [truncated?] 7 [favorited?] 8
			 * [location] 9 [text]
			 */

			String line = value.toString().toLowerCase();
			String[] words = line.split("\t");
			TweetInfo tweet = new TweetInfo();
			tweet.tweetContent = words[words.length - 1];

			String[] tweetWords = tweet.tweetContent.split(" "); // \b
			MapWritable wordCounts = new MapWritable();
			for (String tweetWord : tweetWords) {
				wordCounts.put(new Text(tweetWord), one);
			}

			///TODO: List<String> getWords(String);  contains instead of matches
			
			
			for (Entry<String, Integer> e : trendyHashtags.entrySet()) {
				String tag = e.getKey().substring(1);

				if (Predicates.containsPattern("\\b" + tag + "\\b").apply(
						tweet.tweetContent)) {
					output.collect(new Text(tag), wordCounts);
				}
			}
		}

	}

	/**
	 * REDUCER:
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, MapWritable, Text, Text> {

		private static final int TOP_N_WORDS_LIMIT = 10;

		public void reduce(Text key, Iterator<MapWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// create result wordcount stripe
			TreeMap<String, Integer> wordCounts = Maps.newTreeMap();

			// sum over partial wordcount stripes
			MapWritable mw;
			while (values.hasNext()) {
				mw = values.next();
				for (Entry<Writable, Writable> e : mw.entrySet()) {
					String word = ((Text) e.getKey()).toString();
					int count = ((IntWritable) e.getValue()).get();
					if (wordCounts.containsKey(word))
						wordCounts.put(word, count + 1);
					else
						wordCounts.put(word, count);
				}
			}

			// keep top N entries
			List<String> topNEntries = Lists.newArrayList(wordCounts.keySet());
			if (wordCounts.size() > TOP_N_WORDS_LIMIT)
				topNEntries = topNEntries.subList(0, TOP_N_WORDS_LIMIT);

			output.collect(key,
					new Text(StringUtils.join(topNEntries, ",")));
		}

	}

	static int printUsage() {
		System.out
				.println("stage1 [-m <maps>] [-r <reduces>] <input> <output>");
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
		conf.setJobName("stage1");

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

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new ExtractTopWordsInTrendyTweets(), args);
		System.exit(res);
	}
}
