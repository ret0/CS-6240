package mapreduce.customdatatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class TweetInfo implements Writable {

    public String tweetContent;

    public TweetInfo(String tweetContent) {
        this.tweetContent = tweetContent;
    }
    
    public TweetInfo() {
        this("");
    }

    public void readFields(DataInput in) throws IOException {
        tweetContent = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(tweetContent);
    }

    public String toString() {
        return tweetContent;
    }
    
}
