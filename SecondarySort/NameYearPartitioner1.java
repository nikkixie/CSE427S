package example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NameYearPartitioner<K2, V2> extends
  HashPartitioner<StringPairWritable, Text> {

 
/**
  * Partition Name/Year pairs according to the first string (last name) in the string pair so 
  * that all keys with the same last name go to the same reducer, even if  second part
  * of the key (birth year) is different.
  */
 
 //original partitioner
 
// public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {
//  return (key.getLeft().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
// }
 
 //custom partitioner for 2 reducers
 public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {
  char c = key.getLeft().toLowerCase().charAt(0);
  if((int)c <= (int)'l'){
   return 0;
  }
  else return 1;
 }
 
}
