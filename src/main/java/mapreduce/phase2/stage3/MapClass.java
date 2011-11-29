package mapreduce.phase2.stage3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mapreduce.customdatatypes.IntArrayWriteable;
import mapreduce.customdatatypes.TweetInfo;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Maps;

public class MapClass extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, MapWritable> {

	// constructed in each map task
	private final HashMap<String, Integer> trendyHashtags = new HashMap<String, Integer>();
	private OutputCollector<Text, MapWritable> out;

	// (hashtag -> (user -> count))
	private final Map<String, Map<Long, int[]>> combinerMap = Maps.newHashMap();

	private static final IntWritable ONE = new IntWritable(1);

	public void configure(JobConf conf) {
		try {

			String trendyHashtagsCacheName = new Path(
					conf.get(ExtractTopUserContributionsInTrendyTweets.VARNAME_TRENDY_HASHTAGS_LIST))
					.getName();

			// FOR LOCAL DEBUG
			// loadTrendyHashtags(new Path("data/clusters.txt"));

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
			System.err.println("IOException reading from distributed cache");
			System.err.println(ioe.toString());
		}
	}

	@Override
	public void close() throws IOException {
		for (Entry<String, Map<Long, int[]>> e1 : combinerMap.entrySet()) {

			MapWritable mw = new MapWritable();
			for (Entry<Long, int[]> e2 : e1.getValue().entrySet()) {
				int[] counts = e2.getValue();

				IntArrayWriteable iw = new IntArrayWriteable(
						new IntWritable[] { new IntWritable(counts[0]),
								new IntWritable(counts[1]) });

				mw.put(new Text(e2.getKey().toString()), iw);
			}
			out.collect(new Text(e1.getKey()), mw);

		}
	}

	private void loadTrendyHashtags(Path cachePath) throws IOException {
		BufferedReader lineReader = new BufferedReader(new FileReader(
				cachePath.toString()));
		try {
			String line;
			while ((line = lineReader.readLine()) != null) {
				String[] words = line.split("\t");
				this.trendyHashtags.put(words[1], Integer.parseInt(words[0]));
			}
		} finally {
			lineReader.close();
		}
	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, MapWritable> output, Reporter reporter)
			throws IOException {

		// /FIXME
		out = output;

		TweetInfo tweetInfo = new TweetInfo(value.toString());
		// MapWritable distinctWordsInATweet = new MapWritable();

		Set<String> trendsInThisTweet = tweetInfo.getTrends(trendyHashtags
				.keySet());
		if (!trendsInThisTweet.isEmpty()) {

			long authorId = tweetInfo.getAuthorId();

			HashMap<Long, int[]> authorCounts = Maps.newHashMap();
			authorCounts.put(authorId, new int[] { 1,
					tweetInfo.isRetweet() ? 0 : 1 });

			for (String trend : trendsInThisTweet) {

				if (combinerMap.containsKey(trend))
					combinerMap.put(
							trend,
							combineWordcountMaps(combinerMap.get(trend),
									authorCounts));
				else
					combinerMap.put(trend, authorCounts);

			}

		}
	}

	private Map<Long, int[]> combineWordcountMaps(Map<Long, int[]> map1,
			Map<Long, int[]> map2) {

		Map<Long, int[]> resultMap = Maps.newHashMap();
		resultMap.putAll(map1);

		for (Entry<Long, int[]> e : map2.entrySet()) {
			long key = e.getKey();
			int[] val = e.getValue();
			if (resultMap.containsKey(key)) {
				int[] counts = resultMap.get(key);
				counts[0] += val[0];
				counts[1] += val[1];
				resultMap.put(key, counts);
			} else
				resultMap.put(key, val);
		}

		return resultMap;
	}
}