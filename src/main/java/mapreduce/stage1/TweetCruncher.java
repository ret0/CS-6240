package mapreduce.stage1;

/*
 * Class: 	CS 6240: PARALLEL DATA PROCESSING IN MAPREDUCE
 * Authors: 
 * Title: 	
 * Desc:
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mapreduce.customdatatypes.StringSet;
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
 * Desc:
 * To run:
 * 
 */
public class TweetCruncher extends Configured implements Tool {
	/**
	 * MAPPER:
	 */
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, TweetInfo> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, TweetInfo> output,
				Reporter reporter) throws IOException {
		
			/* 
			 * 0 [tweetid]
			 * 1 [userid] 
			 * 2 [timestamp] 
			 * 3 [reply-tweetid] 
			 * 4 [reply-userid] 
			 * 5 [source] 
			 * 6 [truncated?] 
			 * 7 [favorited?] 
			 * 8 [location] 
			 * 9 [text]
			 */
			String line = value.toString().toLowerCase();
			String[] words = line.split("\t");
			TweetInfo tweet = new TweetInfo();
			tweet.tweetContent = words[words.length-1];
			
			StringSet hashtags = new StringSet();
			
			Pattern hashtagPattern = Pattern.compile("\\B#\\w*[a-zA-Z]+\\w*"); // hashtags with hyphens?
			// http://erictarn.com/post/1060722347/the-best-twitter-hashtag-regular-expression
			Matcher m = hashtagPattern.matcher(tweet.tweetContent);
			while (m.find()) {
				hashtags.set.add(m.group());
			}
			
			// StringSet will be empty for "unclassifiable" tweets
			if(!hashtags.set.isEmpty())
				output.collect(new Text(hashtags.toString()), tweet);
		}
	}

	/**
	 * REDUCER:
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, TweetInfo, Object, Text> {
		public void reduce(Text key, Iterator<TweetInfo> values,
				OutputCollector<Object, Text> output,
				Reporter reporter) throws IOException {


			output.collect(null, key);
			/*
			while(values.hasNext()) {
				TweetInfo tweet = values.next();
				output.collect(key, tweet);
			}
			*/
		}
	}

	static int printUsage() {
		System.out.println("tc [-m <maps>] [-r <reduces>] <input> <output>");
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
		JobConf conf = new JobConf(getConf(), TweetCruncher.class);
		conf.setJobName("tc");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(TweetInfo.class);

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
		int res = ToolRunner.run(new Configuration(), new TweetCruncher(),
				args);
		System.exit(res);
	}
}
