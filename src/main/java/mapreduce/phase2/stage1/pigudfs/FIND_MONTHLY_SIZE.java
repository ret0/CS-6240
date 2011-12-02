package mapreduce.phase2.stage1.pigudfs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import mapreduce.phase2.stage1.NumberOfTrendyTweetsPerDay;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;
import org.joda.time.DateTime;

/**
 * Phase 2, Stage 1: Find number of mentions of top hashtags per day in the "restricted" / interesting date range
 * 
 * Code: UDF for counting number of tweets per month, functionality is now delivered by {@link NumberOfTrendyTweetsPerDay}
 */
public class FIND_MONTHLY_SIZE extends EvalFunc<DataBag> {
	public DataBag exec(Tuple input) throws IOException {
		/*
		 * tweetId: int, userId: int, timestamp: chararray, replyTweetId : int
		 * replyUserId: int source: chararray isTruncated: chararray,
		 * isFavorited: chararray, location: chararray, text: chararray);
		 */

		if (input == null || input.size() == 0 || input.get(input.size() - 1) == null)
			return null;
		try {

			List<String> tags = util.StringTools.splitTagsOnly((String) input
					.get(input.size() - 1));

			Date date = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
					.parse((String) input.get(2));
			DateTime jodaDate = new DateTime(date);

			DataBag tuples = BagFactory.getInstance().newDefaultBag();

			for (String tag : tags) {
				Tuple result = TupleFactory.getInstance().newTuple(4);

				result.set(0, tag);
				result.set(1, jodaDate.getMonthOfYear());
				result.set(2, jodaDate.getYear());
				result.set(3, 1);

				tuples.add(result);
			}

			return tuples;

		} catch (Exception e) {

			throw WrappedIOException.wrap(
					"Caught exception processing" + input.get(input.size() - 1)
							+ " input row ", e);
		}
	}
}