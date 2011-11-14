package mapreduce.customdatatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public abstract class TreeSet<T> implements WritableComparable<TreeSet<T>> {

	protected java.util.TreeSet<T>  set;
	
    public TreeSet() {
        set = new java.util.TreeSet<T>();
    }

    public void readFields(DataInput in) throws IOException {
    	T[] items = readItems(in);
    	this.set.addAll(Arrays.asList(items));
    }

    public void write(DataOutput out) throws IOException {
    	writeItems(out);
    }

    protected abstract T[]  readItems(DataInput in) throws IOException;
    protected abstract void writeItems(DataOutput out);
    
    @Override
    public String toString() {
        String str = "TreeSet [";
        for (T t : this.set) {
			str += t.toString() + ", ";
		}
        str += "]";
        return str;
    }
    
    @Override
    public int hashCode() {
    	return this.set.hashCode();
    }

    @SuppressWarnings("unchecked")
	@Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TreeSet<T> that = (TreeSet<T>) obj;
        return this.set.equals(that.set);
    }

	@Override
	public int compareTo(TreeSet<T> that) {
    	return this.set.equals(that.set)? 0 : ((Integer)this.set.size()).compareTo(that.set.size());
	}

}

