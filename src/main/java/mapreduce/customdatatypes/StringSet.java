package mapreduce.customdatatypes;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;


public class StringSet implements WritableComparable<StringSet> {

	public TreeSet<String>  set;
	
    public StringSet() {
        set = new TreeSet<String>();
    }

    public void readFields(DataInput in) throws IOException {
    	String[] items = in.readUTF().split("\t");
    	this.set.addAll(Arrays.asList(items));
    }

    public void write(DataOutput out) throws IOException {
    	String str= "";
        for (String s : this.set) {
			str += s + "\t";
		}
		out.writeUTF(str);
    }

    @Override
    public String toString() {
        String str = "StringSet [";
        for (String s : this.set) {
			str += s + ", ";
		}
        str += "]";
        return str;
    }
    
    @Override
    public int compareTo(StringSet that) {
    	return this.set.equals(that.set)? 0 : ((Integer)this.set.size()).compareTo(that.set.size());
    }

    
    @Override
    public int hashCode() {
    	return this.set.hashCode();
    }
 

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StringSet that = (StringSet) obj;
        return this.set.equals(that.set);
    }

}
