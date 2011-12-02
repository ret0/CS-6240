package mapreduce.phase1.stage4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

/**
 * Phase 1, Stage 4: Count Co Occurrence of all hashtag permutations of size 2
 * Code: Reducer and Main Function
 */
public class TrendyHashTagPermutations extends Configured implements Tool {

    public static final String VARNAME_TRENDY_HASHTAGS_LIST = "TRENDY_HASHTAGS_LIST";
    public static final String VARNAME_STOPWORDS_LIST = "STOPWORDS_LIST";

    // TRENDY_HASHTAGS_LIST: "/user/venugopalan.s/stage1Out/trendyHashtags.txt"
    // STOPWORDS_LIST: "/user/venugopalan.s/stopwords.txt"

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        private final HashMap<String, Integer> trendyHashtags = new HashMap<String, Integer>();

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

        public void reduce(Text key,
                           Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {

            int tagPairCount = 0;
            while (values.hasNext()) {
                tagPairCount += values.next().get();
            }

            String[] hashtags = key.toString().split(",");
            final String hashtagA = hashtags[0];
            final String hashtagB = hashtags[1];

            int forwardRelationStrength = calculateRelationStrength(tagPairCount, hashtagA);
            int inverseRelationStrength = calculateRelationStrength(tagPairCount, hashtagB);

            if (forwardRelationStrength > 0 || inverseRelationStrength > 0) {
                final IntWritable tagPairScore = new IntWritable(
                        inverseRelationStrength);
                final Integer hashACount = trendyHashtags.get(hashtagA);
                final Integer hashBCount = trendyHashtags.get(hashtagB);
                final Text outVal = new Text(key.toString() + "\t" + tagPairCount
                        + "\t" + hashACount + "\t"
                        + hashBCount + "\t"
                        + forwardRelationStrength);
                output.collect(outVal, tagPairScore);
            }
        }

        private int calculateRelationStrength(int tagPairCount,
                                              final String hashtag) {
            return (int) (((double) tagPairCount / trendyHashtags
                    .get(hashtag)) * 100);
        }
    }

    static int printUsage() {
        System.out.println("stage4 wrong args");
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
        JobConf conf = new JobConf(getConf(), TrendyHashTagPermutations.class);
        conf.setJobName("stage4");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

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
                new TrendyHashTagPermutations(), args);
        System.exit(res);
    }
}
