package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;
import java.util.HashSet;

public class SentimentPartitionTest {

	SentimentPartitioner spart;

	@Test
	public void testSentimentPartition() {

		spart=new SentimentPartitioner();
		spart.setConf(new Configuration());
		int result, result1, result2, result3;		
		
		/*
		 * A test for word "beauty" with expected outcome 0 would   
		 * look like this:
		 */
		result = spart.getPartition(new Text("beauty"), new IntWritable(23), 3);
		assertEquals(result,0);	

		//assert love is positive
		result1 = spart.getPartition(new Text("love"), new IntWritable(23), 3);
		assertEquals(result1,0);	
		//assert deadly is negative
		result2 = spart.getPartition(new Text("deadly"), new IntWritable(23), 3);
		assertEquals(result2,1);	
		//assert zodiac is neutral
		result3 = spart.getPartition(new Text("zodiac"), new IntWritable(23), 3);
		assertEquals(result3,2);	
		
	}

}