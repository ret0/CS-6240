package mapreduce.customdatatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;

public abstract class Stripe<K, V> implements Writable {

	protected java.util.HashMap<K, V> map;

	public java.util.HashMap<K, V> getMap() {
		return this.map;
	}

	public Stripe() {
		map = new java.util.HashMap<K, V>();
	}

	public void readFields(DataInput in) {
		readItems(in);
	}

	public void write(DataOutput out) {
		writeItems(out);
	}

	protected abstract void readItems(DataInput in);

	protected abstract void writeItems(DataOutput out);

	@Override
	public String toString() {
		String str = "Stripe [";
		for (Entry<K, V> e : this.map.entrySet()) {
			str += "(" + e.getKey() + " -> " + e.getValue() + "), ";
		}
		str += "]";
		return str;
	}

	public void filter(int topN, Comparator<Entry<K, V>> comp) {
		SortedSet<Entry<K, V>> orderedEntries = new java.util.TreeSet<Entry<K, V>>(
				comp);
		orderedEntries.addAll(this.map.entrySet());

		ArrayList<Entry<K, V>> list = (ArrayList<Entry<K, V>>) Lists
				.newArrayList(orderedEntries).subList(0, topN);

		this.map.clear();
		for (Entry<K, V> e : list) {
			this.map.put(e.getKey(), e.getValue());
		}
	}
}
