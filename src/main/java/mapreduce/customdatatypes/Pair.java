package mapreduce.customdatatypes;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;

public abstract class Pair<K, V> implements Writable {

	protected K key;
	protected V value;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public Pair() {
		this(null, null);
	}

	abstract public void readFields(DataInput in);

	abstract public void write(DataOutput out);

	@Override
	public String toString() {
		return "Pair [key=" + this.key + ", value=" + this.value + "]";
	}
}
