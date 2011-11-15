package mapreduce.stage4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mapreduce.customdatatypes.TweetInfo;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Permutations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class MapClass extends MapReduceBase implements
		Mapper<LongWritable, Text, TextWritable, IntWritable> {

	// constructed in each map task
	private final HashMap<String, Integer> trendyHashtags = new HashMap<String, Integer>();
	private OutputCollector<TextWritable, IntWritable> out;
	
	private final Map<Set<String>, Integer> combinerMap = Maps.newHashMap();

	public void configure(JobConf conf) {
		try {
			
			String trendyHashtagsCacheName = new Path(
					conf.get(TrendyHashTagPermutations.VARNAME_TRENDY_HASHTAGS_LIST))
					.getName();

			// FOR  LOCAL  DEBUG
			loadTrendyHashtags(new Path("data/clusters.txt")); 

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
		for (Entry<Set<String>, Integer> e1 : combinerMap.entrySet()) {
		    List<String> bothTags = Lists.newArrayList(e1.getKey());
		    Text hashA = new Text(bothTags.get(0));
		    Text countA = new Text(String.valueOf(trendyHashtags.get(hashA.toString())));
		    Text hashB = new Text(bothTags.get(1));
		    Text countB = new Text(String.valueOf(trendyHashtags.get(hashB.toString())));
		    final TextWritable outKey = new TextWritable();
            outKey.set(new Text[] {hashA, countA, hashB, countB});
            out.collect(outKey, new IntWritable(e1.getValue()));
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
			OutputCollector<TextWritable, IntWritable> output, Reporter reporter)
			throws IOException {

		///FIXME
		out = output;
		
		TweetInfo tweetInfo = new TweetInfo(value.toString());

		Set<String> distinctTagsInSingleTweet = Sets.newTreeSet(tweetInfo.getAllHashtags());
		
		final SetView<String> allTrendyTagsInASingleTweet = Sets.intersection(trendyHashtags.keySet(), distinctTagsInSingleTweet);
        if(allTrendyTagsInASingleTweet.size() > 1) {
            Permutations<String> perm = new Permutations<String>(Lists.newArrayList(allTrendyTagsInASingleTweet), 2);
            while (perm.hasNext()) {
                Set<String> permSet = Sets.newTreeSet(perm.next());
                if(combinerMap.containsKey(permSet)) {
                    combinerMap.put(permSet, combinerMap.get(permSet) + 1);
                } else {
                    combinerMap.put(permSet, 1);
                }
            }
		}
	}

}