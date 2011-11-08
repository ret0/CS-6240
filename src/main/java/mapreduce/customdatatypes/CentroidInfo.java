package mapreduce.customdatatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CentroidInfo implements WritableComparable<CentroidInfo> {

    private int bestCentroidID = -1;
    private String tweetContent = "";

    public CentroidInfo(int bestCentroidID, String tweetContent) {
        this.bestCentroidID = bestCentroidID;
        this.tweetContent = tweetContent;
    }

    public CentroidInfo() {
        this(-1, "");
    }
    
    public void readFields(DataInput in) throws IOException {
        bestCentroidID = in.readInt();
        tweetContent = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(bestCentroidID);
        out.writeUTF(tweetContent);
    }

    @Override
    public String toString() {
        return "CentroidInfo [bestCentroidID=" + bestCentroidID
                + ", tweetContent=" + tweetContent + "]";
    }

    @Override
    public int compareTo(CentroidInfo obj) {
        return new Integer(this.bestCentroidID).compareTo(obj.bestCentroidID);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + bestCentroidID;
        result = prime * result + ((tweetContent == null) ? 0 : tweetContent.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CentroidInfo other = (CentroidInfo) obj;
        if (bestCentroidID != other.bestCentroidID)
            return false;
        if (tweetContent == null) {
            if (other.tweetContent != null)
                return false;
        } else if (!tweetContent.equals(other.tweetContent))
            return false;
        return true;
    }
    
    
    

}
