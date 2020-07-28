package com.proejctgr.NumberCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class NumberSumDriver {
	
	public static void main(String[] args) throws Exception
	  {
	    Configuration conf = new Configuration();
	    String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    
	    if (pathArgs.length < 2)
	    {
	      System.err.println("MR Project Usage: wordcount <input-path> [...] <output-path>");
	      System.exit(2);
	    }
	    
	    Job numSumJob = Job.getInstance(conf, "MapReduce NumberSum");
	    numSumJob.setJarByClass(NumberSumDriver.class);
	    numSumJob.setMapperClass(NumberMapper.class);
	    numSumJob.setCombinerClass(NumberReducer.class);
	    numSumJob.setReducerClass(NumberReducer.class);
	  
	    numSumJob.setOutputKeyClass(LongWritable.class);
	    numSumJob.setOutputValueClass(FloatWritable.class);
	    
	    for (int i = 0; i < pathArgs.length - 1; ++i)
	    {
	      FileInputFormat.addInputPath(numSumJob, new Path(pathArgs[i]));
	    }
	    
	    FileOutputFormat.setOutputPath(numSumJob, new Path(pathArgs[pathArgs.length - 1]));
	    System.exit(numSumJob.waitForCompletion(true) ? 0 : 1);
	  }

}
