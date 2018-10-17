package solution;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//specify objects we can reuse(the wordObject and the letter length object)
	private Text wordObject = new Text();
	private IntWritable letterLength = new IntWritable();
	//this is the param we passed in in command line 
	private boolean myCaseSensitive;

	//the setup 
	@Override
	public void setup(Context context)
		throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		 myCaseSensitive = conf.getBoolean("caseSensitive", true);
	}
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  
	  for(String word : line.split("\\W+")){
		  if(word.length() > 0){
			  int length = word.length();
		  	String cs = word.substring(0,1);
			  if(myCaseSensitive == false){
				 cs = cs.toLowerCase();
			  }
			  wordObject.set(cs);
			  letterLength.set(length);
			  context.write(wordObject, letterLength);
		  }
			  
	  }

  }
}
