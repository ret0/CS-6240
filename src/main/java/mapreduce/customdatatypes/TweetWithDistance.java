package mapreduce.customdatatypes;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 */
public class TweetWithDistance implements Writable {

    public String tweetContent;
    public double distance;

    public TweetWithDistance(String tweetContent, double dist) {
        this.tweetContent = tweetContent;
        this.distance = dist;
    }
    
    public TweetWithDistance() {
        this("", -1);
    }

    public void readFields(DataInput in) throws IOException {
        tweetContent = in.readUTF();
        distance = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(tweetContent);
        out.writeDouble(distance);
    }

    public String toString() {
        return tweetContent + " >>Dist=" + String.valueOf(distance);
    }
    
}
