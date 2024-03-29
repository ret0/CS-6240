package mapreduce.phase2.stage2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

import util.MapSorter;

import com.google.common.collect.Maps;

/**
 * Phase 2, Stage 2: Find the most often mentioned users within a certain topic cluster
 * 
 * Code: Reducer / Main
 */

public class ExtractTopUserMentionsInTrendyTweets extends Configured implements
        Tool {

    public static final String VARNAME_TRENDY_HASHTAGS_LIST = "TRENDY_HASHTAGS_LIST";

    // TRENDY_HASHTAGS_LIST: "/user/venugopalan.s/stage1Out/trendyHashtags.txt"

    /**
     * REDUCER:
     */
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, MapWritable, Text, Text> {

        private static final int TOP_USERS_LIMIT = 50;

        public void reduce(Text key,
                           Iterator<MapWritable> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {

            // create result usercount stripe
            Map<String, Integer> mergedUserCounts = Maps.newHashMap();
            // sum over partial usercount stripes
            MapWritable mw;
            while (values.hasNext()) {
                mw = values.next();

                for (Entry<Writable, Writable> e : mw.entrySet()) {
                    String word = ((Text) e.getKey()).toString();
                    int count = ((IntWritable) e.getValue()).get();
                    if (mergedUserCounts.containsKey(word))
                        mergedUserCounts.put(word, count + 1);
                    else
                        mergedUserCounts.put(word, count);
                }
            }

            // get top N entries
            Map<String, Integer> topNUserCounts = Maps.newHashMap();
            for (int i = 0; i < TOP_USERS_LIMIT; i++) {
                int maxVal = -1;
                String maxKey = "";
                for (Entry<String, Integer> e : mergedUserCounts.entrySet()) {
                    if (e.getValue() > maxVal) {
                        maxVal = e.getValue();
                        maxKey = e.getKey();
                    }
                }
                mergedUserCounts.remove(maxKey);
                topNUserCounts.put(maxKey, maxVal);
            }
            topNUserCounts = new MapSorter<String, Integer>()
                    .sortByValue(topNUserCounts);
            mergedUserCounts = null;

            final Text valUserList = new Text(StringUtils.join(topNUserCounts.entrySet(), ","));
            output.collect(key, valUserList);
        }
    }

    static int printUsage() {
        System.out
                .println("phase2-stage2 [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(),
                ExtractTopUserMentionsInTrendyTweets.class);
        conf.setJobName("phase2-stage2");

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
                new ExtractTopUserMentionsInTrendyTweets(), args);
        System.exit(res);
    }
}
