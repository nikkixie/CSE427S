package stubs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;


public class SentimentPartitioner extends org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
	  //add all words to my hashset 
	  this.configuration = configuration;
	  Scanner negative_file;
	  //if success, proceed 
	try {
		negative_file = new Scanner(new File("negative-words.txt"));
		  while(negative_file.hasNext()){
			  negative.add(negative_file.next().trim().toLowerCase());
		  }
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	}
	//add all positive words 
	  Scanner file;
	try {
		file = new Scanner (new File("positive-words.txt"));
		  while(file.hasNext()){
			  positive.add(file.next().trim().toLowerCase());
		  }
	} catch (FileNotFoundException e) {

		e.printStackTrace();
	} 


	  
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }


  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	 //if word is in positive txt, then partition to 0
	  if(positive.contains(key.toString().toLowerCase())){
		return 0;
		//else partition it to 1
	 }else if(negative.contains(key.toString().toLowerCase())){
		 return 1;
		 //else partition it to 2
	 }else {return 2;}
  }





}