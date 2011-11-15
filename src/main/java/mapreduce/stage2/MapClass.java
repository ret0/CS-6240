package mapreduce.stage2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

	private final Map<String, Map<String, Integer>> combinerMap = Maps
			.newHashMap();

	private static final IntWritable ONE = new IntWritable(1);

	public void configure(JobConf conf) {
		try {

			String trendyHashtagsCacheName = new Path(
					conf.get(ExtractTopWordsInTrendyTweets.VARNAME_TRENDY_HASHTAGS_LIST))
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
		for (Entry<String, Map<String, Integer>> e1 : combinerMap.entrySet()) {

			MapWritable mw = new MapWritable();
			for (Entry<String, Integer> e2 : e1.getValue().entrySet()) {
				mw.put(new Text(e2.getKey()), new IntWritable(e2.getValue()));
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
		Map<String, Integer> distinctWordsInATweet = Maps.newHashMap();

		for (String tweetWord : tweetInfo.getAllMeaningfulWords()) {
			distinctWordsInATweet.put(tweetWord, 1);
			// TODO how many times do we count a word?
		}

		for (String word : distinctWordsInATweet.keySet()) {
			if (word.startsWith("#") && trendyHashtags.containsKey(word)) {
				// output.collect(w, distinctWordsInATweet);
				if (!combinerMap.containsKey(word))
					combinerMap.put(word, distinctWordsInATweet);
				else
					combinerMap.put(
							word,
							combineWordcountMaps(combinerMap.get(word),
									distinctWordsInATweet));
			}
		}
	}

	private Map<String, Integer> combineWordcountMaps(
			Map<String, Integer> map1, Map<String, Integer> map2) {

		Map<String, Integer> resultMap = Maps.newHashMap();
		resultMap.putAll(map1);

		for (Entry<String, Integer> e : map2.entrySet()) {
			String key = e.getKey();
			Integer val = e.getValue();
			if (resultMap.containsKey(key))
				resultMap.put(key, resultMap.get(key) + val);
			else
				resultMap.put(key, val);
		}

		return resultMap;
	}
}