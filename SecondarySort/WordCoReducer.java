package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCoReducer  extends Reducer<StringPairWritable, LongWritable, Text, LongWritable> {


	  @Override
		public void reduce(StringPairWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			int wordCount = 0;
			String left = key.left;
			String right = key.right;
			String ret = "(" + left + "," + right + ")";
			
					
			/*
			 * For each value in the set of values passed to us by the mapper:
			 */
			for (LongWritable value : values) {
			  
			  /*
			   * Add the value to the word count counter for this key.
			   */
				wordCount += value.get();
			}
			
			/*
			 * Call the write method on the Context object to emit a key
			 * and a value from the reduce method. 
			 */
			context.write(new Text(ret), new LongWritable(wordCount));
		}
	}