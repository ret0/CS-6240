package mapreduce.stage2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import mapreduce.customdatatypes.TweetInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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

import util.MapSorter;

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

		private static final IntWritable ONE = new IntWritable(1);
	
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
		    
		    TweetInfo tweetInfo = new TweetInfo(value.toString());
            MapWritable distinctWordsInATweet = new MapWritable();

			for (String tweetWord : tweetInfo.getAllWords()) {
				distinctWordsInATweet.put(new Text(tweetWord), ONE);
				// TODO how many times do we count a word?
			}

			for (Writable word : distinctWordsInATweet.keySet()) {
                Text w = (Text) word;
                if(w.toString().startsWith("#") && trendyHashtags.containsKey(w.toString())) {
                    output.collect(w, distinctWordsInATweet);
                }
            }
		}
	}

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
			MapSorter<String, Integer> ms = new MapSorter<String, Integer>();
			Map<String, Integer> sortedWordCounts = ms.sortByValue(wordCounts);
			List<String> topNEntries = Lists.newArrayList();
			Iterator<String> it = sortedWordCounts.keySet().iterator();
			int i = 0;
			while(it.hasNext() && i<TOP_N_WORDS_LIMIT){
			    topNEntries.add(it.next());
			    i++;
			}
			
			output.collect(key, new Text(StringUtils.join(topNEntries, ",")));
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

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new ExtractTopWordsInTrendyTweets(), args);
		System.exit(res);
	}
}
