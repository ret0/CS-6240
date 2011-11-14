package mapreduce.customdatatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;

public class WordCountMap extends Stripe<String, Integer> {

	@Override
	protected void readItems(DataInput in) {
		try {
			int size = in.readInt();
			while (size > 0) {
				this.map.put(in.readUTF(), in.readInt());
				size--;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void writeItems(DataOutput out) {
		try {
			out.writeInt(this.map.size());
			for (Entry<String, Integer> e : this.map.entrySet()) {
				out.writeUTF(e.getKey());
				out.writeInt(e.getValue());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

