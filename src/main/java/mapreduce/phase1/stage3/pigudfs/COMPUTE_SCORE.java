package mapreduce.phase1.stage3.pigudfs;

import java.io.IOException;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class COMPUTE_SCORE extends EvalFunc<Tuple> {
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			Tuple result = TupleFactory.getInstance().newTuple(3);

			String tag1 = (String) input.get(0);
			Set<String> words1 = Sets.newHashSet(Lists.newArrayList(((String)input.get(1)).split(",")));
			String tag2 = (String) input.get(2);
			Set<String> words2 = Sets.newHashSet(Lists.newArrayList(((String)input.get(3)).split(",")));

			result.set(0, tag1);
			result.set(1, tag2);
			result.set(2, Sets.intersection(words1, words2).size() );

			return result;

		} catch (Exception e) {
			throw WrappedIOException.wrap(
					"Caught exception processing input row ", e);
		}
	}
}