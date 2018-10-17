package solution;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgWordLength extends Configured implements Tool {
	//we use toolrunner to run the pragram instead of running it in main (as we did without toolrunner implemented)
	public static void main(String[] args) throws Exception{
		int ret = ToolRunner.run(new Configuration(), new AvgWordLength(), args);
		System.exit(ret);
	}


	@Override	
  public int run(String[] args) throws Exception {

   //check to see if two valid arguments are passed in as input file path and output file path
    if (args.length != 2) {
    System.out.printf( "Usage: %s [generic options] <input dir> <output dir>\n”, getClass().getSimpleName()"); 
      System.exit(-1);
    }

    //get configuration
	Configuration conf = this.getConf();
	
	//initialize job with retrieved configuration
	Job job = new Job(conf);

    //specify jar file and name 
    job.setJarByClass(AvgWordLength.class);
    job.setJobName("Average Word Length");
    //specify input path and output path got from arguments 
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    //specify mapper and reducer class 
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);
    //specify output key and value type of mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    //specify output key and value type of reducers 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
/*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    return job.waitForCompletion(true) ? 0 : 1;
  }
}

