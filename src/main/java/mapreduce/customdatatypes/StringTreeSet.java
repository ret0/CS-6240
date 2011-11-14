package mapreduce.customdatatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class StringTreeSet extends TreeSet<String>{

	@Override
	protected String[] readItems(DataInput in){
		try {
			return in.readUTF().split("\t");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected void writeItems(DataOutput out) {
    	String str= "";
        for (String s : this.set) {
			str += s + "\t";
		}
		try {
			out.writeUTF(str);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
