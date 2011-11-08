package mapreduce;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;

import mapreduce.customdatatypes.CentroidInfo;
import mapreduce.customdatatypes.TweetWithDistance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.Maps;

import cs6240.twitterProject.App;

/**
 * 
 */
public class KMeansNoIterations {

    public static class EquiJoinMapper extends
            Mapper<Object, Text, CentroidInfo, TweetWithDistance> {

        private Map<Integer, String> initialCentroids = Maps.newHashMap();

        public EquiJoinMapper() throws IOException {
            Path pt = new Path("data/SMSSpamCollectionHamInitialCentroids.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    fs.open(pt)));
            String line = br.readLine();
            int lineIndex = 0;
            while (line != null) {
                initialCentroids.put(lineIndex++, line);
                line = br.readLine();
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException,
                InterruptedException {

            double bestDistance = 0;
            int bestCentroidID = -1;

            for (Entry<Integer, String> initialCentroid : initialCentroids
                    .entrySet()) {
                double fuzzyDistance = App.fuzzyDistance(value.toString(),
                        initialCentroid.getValue());
                if (fuzzyDistance > bestDistance) {
                    bestDistance = fuzzyDistance;
                    bestCentroidID = initialCentroid.getKey();
                }
            }
            
            if(bestCentroidID != -1) {
                context.write(new CentroidInfo(bestCentroidID, initialCentroids.get(bestCentroidID)),
                        new TweetWithDistance(value.toString(), bestDistance));
            }
        }
    }

    /**
     */
    public static class EquiJoinReducer extends
            Reducer<CentroidInfo, TweetWithDistance, Object, Text> {

        public void reduce(CentroidInfo key,
                           Iterable<TweetWithDistance> values,
                           Context context)
                throws IOException,
                InterruptedException {

            context.write(null, new Text("\n" + key));
            for (TweetWithDistance tweetWithDistance : values) {
                context.write(null, new Text(tweetWithDistance.toString()));
            }
        }

    }

    public static void main(String[] args)
            throws IOException,
            InterruptedException,
            ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: equijoin <in1> <out>");
            System.exit(2);
        }
        Job job = configureJob(conf, otherArgs[0], otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Job configureJob(Configuration conf,
                                    String inputPath1,
                                    String outputPath) throws IOException {
        Job job = new Job(
                conf,
                "simple mr k means, no iterations");
        job.setJarByClass(KMeansNoIterations.class);

        job.setMapperClass(EquiJoinMapper.class);
        job.setReducerClass(EquiJoinReducer.class);

        job.setMapOutputKeyClass(CentroidInfo.class);
        job.setMapOutputValueClass(TweetWithDistance.class);

        job.setOutputValueClass(Text.class);

        //job.setNumReduceTasks(TOTAL_NUMBER_OF_CORES);
        //job.setPartitionerClass(ModuloPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(inputPath1));
        //FileInputFormat.addInputPath(job, new Path(inputPath2));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

}
