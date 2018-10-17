package stubs;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text Str = new Text();
	private Text match = new Text();
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
	   //get the file name for use later 
	  String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
	    
	  // the first extracted word must be the line 
	    int count = 0;
	    String line = "";
	    //split line inputs into words 
	    for(String word : value.toString().split("\\W+")){
	    	if(word.length()>0){
	    		//the first must be line 
	    		if(count == 0){
	    			line = word;
	    			count++;
	    			continue;
	    		}else{
	    			//later ones are all words that appear in the literature
	    			String textwords = word.toLowerCase();
			    	Str.set(textwords);
			    	//filename @ line on which the textwords above appear
			    	String ret = filename+ "@" + line;
			    	match.set(ret);
			    	context.write(Str, match);
			    	count++;
	    		}
	    	}
	    }
	   	    
    
  }
}