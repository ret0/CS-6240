package mapreduce.phase2.stage1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import mapreduce.customdatatypes.TweetInfo;
import mapreduce.phase1.stage2.ExtractTopWordsInTrendyTweets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;

import com.google.common.collect.Maps;

/**
 * Phase 2, Stage 1: Find number of mentions of top hashtags per day in the "restricted" / interesting date range
 * 
 * Code: Mapper / Reducer / Main
 */

public class NumberOfTrendyTweetsPerDay extends Configured implements Tool {

    public static class MapClass extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        private static final DateMidnight START_DATE = new DateMidnight(2008, 10, 1);
        private static final DateMidnight END_DATE = new DateMidnight(2009, 9, 1);
        
        private final HashMap<String, Integer> trendyHashtags = new HashMap<String, Integer>();
        private final HashMap<String, Integer> combiner = Maps.newHashMap();
        
        private OutputCollector<Text, IntWritable> out;

        /**
         * Mapper uses dummy key to count total tweets per day (used to normalize data)
         */
        public void map(LongWritable key,
                        Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            
            out = output;

            TweetInfo tweetInfo = new TweetInfo(value.toString());
            final DateTime tweetDateTime = tweetInfo.getTweetDateTime();

            if (tweetDateTime.isAfter(START_DATE)
                    && tweetDateTime.isBefore(END_DATE)) {

                final String simpleDate = getSimpleDate(tweetDateTime);
                final String dummyKey = "aaa;" + simpleDate;
                increaseCombinerValue(dummyKey);

                for (String hashTag : tweetInfo.getAllHashtags()) {
                    if (trendyHashtags.containsKey(hashTag)) {
                        final String emitKey = hashTag + ";" + simpleDate;
                        increaseCombinerValue(emitKey);
                        break;
                    }
                }
            }
        }

        private void increaseCombinerValue(final String emitKey) {
            if(combiner.containsKey(emitKey)) {
                combiner.put(emitKey, combiner.get(emitKey) + 1);
            } else {
                combiner.put(emitKey, 1);
            }
        }
        
        public void close() throws IOException {
            for (Entry<String, Integer> entry : combiner.entrySet()) {
                out.collect(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }

        private String getSimpleDate(final DateTime tweetDateTime) {
            return tweetDateTime.getYear() + "-"
                    + String.format("%02d", tweetDateTime.getMonthOfYear())
                    + "-"
                    + String.format("%02d", tweetDateTime.getDayOfMonth());
        }

        public void configure(JobConf conf) {
            try {

                String trendyHashtagsCacheName = new Path(
                        conf.get(ExtractTopWordsInTrendyTweets.VARNAME_TRENDY_HASHTAGS_LIST))
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
    }

    /**
     * REDUCER:
     */
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key,
                           Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {

            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
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
     */
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(),
                NumberOfTrendyTweetsPerDay.class);
        conf.setJobName("Phase 2, stage1 - hashtag per Month");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(Reduce.class);

        if (args.length != 3) {
            System.out.println("ERROR: Wrong number of parameters: "
                    + args.length + " instead of 3.");
            System.out.println("INPUT OUTPUT TRENDYHASHTAGS-PATH");
            return printUsage();
        }
        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.set(ExtractTopWordsInTrendyTweets.VARNAME_TRENDY_HASHTAGS_LIST,
                args[2]);
        DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new NumberOfTrendyTweetsPerDay(), args);
        System.exit(res);
    }
}
