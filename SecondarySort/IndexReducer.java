package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  //use a stringbuilder to concatenate all the words appeared in different works 
	  StringBuilder sb = new StringBuilder();
	  for(Text value : values){
		  if(sb.length() != 0) sb.append(",");
		  sb.append(value.toString());
	  }
	  //record holds all the places where the word appears 
	  Text rcd = new Text();
	  rcd.set(sb.toString());
	  context.write(key, rcd);
    
  }
}