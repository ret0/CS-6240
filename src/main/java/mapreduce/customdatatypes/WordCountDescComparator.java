package mapreduce.customdatatypes;

import java.util.Comparator;
import java.util.Map.Entry;


public class WordCountDescComparator implements Comparator<Entry<String, Integer>> {

	@Override
	public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
		return o2.getValue().compareTo(o1.getValue());
	}

}
