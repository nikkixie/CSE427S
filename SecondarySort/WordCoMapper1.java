package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCoMapper extends
    Mapper<LongWritable, Text, StringPairWritable, LongWritable> {
	LongWritable reuse = new LongWritable(1);

	  @Override
	  public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {

			  String line = value.toString();
			  
			  String[] list = line.split("\\W+");
			 
			  if(list.length > 0){
				  String left = list[0];
				  String right = list[1];
				  StringPairWritable concat = new StringPairWritable(left, right);
				  context.write(concat, reuse);
			  }
	  }
}
