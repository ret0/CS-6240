package mapreduce.phase1.stage4;

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

/**
 * Phase 1, Stage 4: Count Co Occurrence of all hashtag permutations of size 2
 * Code: Mapper
 */

public class MapClass extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {

    private final HashMap<String, Integer> trendyHashtags = new HashMap<String, Integer>();
    private OutputCollector<Text, IntWritable> out;

    private final Map<Set<String>, Integer> combinerMap = Maps.newHashMap();

    public void configure(JobConf conf) {
        try {

            String trendyHashtagsCacheName = new Path(
                    conf.get(TrendyHashTagPermutations.VARNAME_TRENDY_HASHTAGS_LIST))
                    .getName();

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
        for (Entry<Set<String>, Integer> e : combinerMap.entrySet()) {
            List<String> tagPair = Lists.newArrayList(e.getKey());
            final Text outKey = new Text(tagPair.get(0) + "," + tagPair.get(1));
            out.collect(outKey, new IntWritable(e.getValue()));
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

    /**
     * Emit (via in mapper combiner) all permutations of length 2
     * if a tweet contains 2 or more hashtags
     */
    public void map(LongWritable key,
                    Text value,
                    OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {

        out = output;

        TweetInfo tweetInfo = new TweetInfo(value.toString());

        Set<String> distinctTagsInSingleTweet = Sets.newTreeSet(tweetInfo
                .getAllHashtags());

        Set<String> trendyTagsInASingleTweet = Sets.intersection(
                trendyHashtags.keySet(), distinctTagsInSingleTweet);

        if (trendyTagsInASingleTweet.size() > 1) {
            Permutations<String> perm = new Permutations<String>(
                    Lists.newArrayList(trendyTagsInASingleTweet), 2);

            while (perm.hasNext()) {
                Set<String> tagPair = Sets.newTreeSet(perm.next());
                if (combinerMap.containsKey(tagPair)) {
                    combinerMap.put(tagPair, combinerMap.get(tagPair) + 1);
                } else {
                    combinerMap.put(tagPair, 1);
                }
            }
        }

    }

}