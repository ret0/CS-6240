package mapreduce.phase2.stage3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mapreduce.customdatatypes.IntArrayWriteable;

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

/**
 * Phase 2, Stage 3: Determine the number of contributions a user was made
 * within a topic cluster. The contributions are divided into
 * "original contributions" and Retweets
 * 
 * Code: Reducer / Main
 * 
 */
public class ExtractTopUserContributionsInTrendyTweets extends Configured
        implements Tool {

    public static final String VARNAME_TRENDY_HASHTAGS_LIST = "TRENDY_HASHTAGS_LIST";
    public static final int TOTAL_CONTRIB_INDEX = 0;
    public static final int ORIGINAL_CONTRIB_INDEX = 1;

    // TRENDY_HASHTAGS_LIST: "/user/venugopalan.s/stage1Out/trendyHashtags.txt"

    /**
     * REDUCER:
     */
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, MapWritable, Text, Text> {

        private static final String OUTPUT_SEPARATOR = "\t";

        private static final int TOP_N_USERS_LIMIT = 50;

        private final HashMap<String, Integer> trendyHashtags = new HashMap<String, Integer>();

        public void configure(JobConf conf) {
            try {

                String trendyHashtagsCacheName = new Path(
                        conf.get(ExtractTopUserContributionsInTrendyTweets.VARNAME_TRENDY_HASHTAGS_LIST))
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
                    String[] words = line.split(OUTPUT_SEPARATOR);
                    this.trendyHashtags.put(words[1],
                            Integer.parseInt(words[0]));
                }
            } finally {
                lineReader.close();
            }
        }

        public void reduce(Text key,
                           Iterator<MapWritable> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {

            // create result usercount stripe
            Map<String, int[]> mergedUserCounts = Maps.newHashMap();
            // sum over partial usercount stripes
            MapWritable mw;
            while (values.hasNext()) {
                mw = values.next();

                for (Entry<Writable, Writable> e : mw.entrySet()) {
                    String user = ((Text) e.getKey()).toString();
                    Writable[] userContributions = ((IntArrayWriteable) e
                            .getValue()).get();
                    int totalAuthored = ((IntWritable) userContributions[TOTAL_CONTRIB_INDEX])
                            .get();
                    int originalAuthored = ((IntWritable) userContributions[ORIGINAL_CONTRIB_INDEX])
                            .get();

                    if (!mergedUserCounts.containsKey(user))
                        mergedUserCounts.put(user, new int[] { totalAuthored,
                                originalAuthored });
                    else {
                        int[] vals = mergedUserCounts.get(user);
                        vals[TOTAL_CONTRIB_INDEX] += totalAuthored;
                        vals[ORIGINAL_CONTRIB_INDEX] += originalAuthored;
                        mergedUserCounts.put(user, vals);
                    }
                }
            }

            Map<String, int[]> removedElements = Maps.newHashMap();

            /**
             * Find the users the made the most total and most "unique"
             * contributions
             */
            findTopTotalContributors(key, output, mergedUserCounts,
                    removedElements);

            mergedUserCounts.putAll(removedElements);
            removedElements = null;

            findTopTotalOriginalContributors(key, output, mergedUserCounts);
            mergedUserCounts = null;
        }

        private void findTopTotalOriginalContributors(Text key,
                                                       OutputCollector<Text, Text> output,
                                                       Map<String, int[]> mergedUserCounts)
                throws IOException {
            int[] counts;
            for (int i = 0; i < TOP_N_USERS_LIMIT; i++) {
                String maxOriginalContribKey = "";
                int maxOriginalContribVal = -1;

                for (Entry<String, int[]> e : mergedUserCounts.entrySet()) {
                    counts = e.getValue();
                    if (counts[ORIGINAL_CONTRIB_INDEX] > maxOriginalContribVal) {
                        maxOriginalContribVal = counts[ORIGINAL_CONTRIB_INDEX];
                        maxOriginalContribKey = e.getKey();
                    }
                }
                if (maxOriginalContribVal > -1) {
                    counts = mergedUserCounts.remove(maxOriginalContribKey);
                    output.collect(
                            key,
                            createContributionOutputValue(key, counts,
                                    maxOriginalContribKey, "oc"));
                }
            }
        }

        private void findTopTotalContributors(Text key,
                                               OutputCollector<Text, Text> output,
                                               Map<String, int[]> mergedUserCounts,
                                               Map<String, int[]> removedElements)
                throws IOException {
            // get top N entries
            int[] counts;
            for (int i = 0; i < TOP_N_USERS_LIMIT; i++) {

                String maxTotalContribKey = "";
                int maxTotalContribVal = -1;

                for (Entry<String, int[]> e : mergedUserCounts.entrySet()) {
                    counts = e.getValue();
                    if (counts[TOTAL_CONTRIB_INDEX] > maxTotalContribVal) {
                        maxTotalContribVal = counts[TOTAL_CONTRIB_INDEX];
                        maxTotalContribKey = e.getKey();
                    }
                }
                if (maxTotalContribVal > -1) {
                    counts = mergedUserCounts.remove(maxTotalContribKey);
                    removedElements.put(maxTotalContribKey, counts);
                    output.collect(
                            key,
                            createContributionOutputValue(key, counts,
                                    maxTotalContribKey, "tc"));
                }
            }
        }

        private Text createContributionOutputValue(Text key,
                                                   int[] counts,
                                                   String maxTotalContribKey,
                                                   String label) {
            return new Text(
                    maxTotalContribKey
                            + OUTPUT_SEPARATOR
                            + label
                            + OUTPUT_SEPARATOR
                            + counts[TOTAL_CONTRIB_INDEX]
                            + OUTPUT_SEPARATOR
                            + counts[ORIGINAL_CONTRIB_INDEX]
                            + OUTPUT_SEPARATOR
                            + calcPercentContribution(counts[TOTAL_CONTRIB_INDEX], this.trendyHashtags
                                                .get(key.toString()))
                            + OUTPUT_SEPARATOR
                            + calcPercentContribution(counts[ORIGINAL_CONTRIB_INDEX], counts[TOTAL_CONTRIB_INDEX]));
        }

        private double calcPercentContribution(int count, int divisor) {
            return ((double) count / (double) divisor) * 100.0;
        }
    }

    static int printUsage() {
        System.out
                .println("phase2-stage3 [-m <maps>] [-r <reduces>] <input> <output>");
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
                ExtractTopUserContributionsInTrendyTweets.class);
        conf.setJobName("phase2-stage3");

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
                new ExtractTopUserContributionsInTrendyTweets(), args);
        System.exit(res);
    }
}
