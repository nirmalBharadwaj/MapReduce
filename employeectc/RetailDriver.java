package com.proejctgr.employeectc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RetailDriver {
	
	public static void main(String[] args) throws Exception
	  {
	    Configuration conf = new Configuration();
	    String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    
	    if (pathArgs.length < 2)
	    {
	      System.err.println("MR Project Usage: wordcount <input-path> [...] <output-path>");
	      System.exit(2);
	    }
	    
	    Job retailJob = Job.getInstance(conf, "MapReduce Employee Data Analysis");
	    retailJob.setJarByClass(RetailDriver.class);
	    retailJob.setMapperClass(RetailMapper.class);
	    retailJob.setReducerClass(RetailReducer.class);
	    retailJob.setPartitionerClass(RetailPartitioner.class);
	  
	    retailJob.setMapOutputKeyClass(Text.class);
	    retailJob.setMapOutputValueClass(Text.class);
	    retailJob.setOutputKeyClass(NullWritable.class);
	    retailJob.setOutputValueClass(Text.class);
	    
	    for (int i = 0; i < pathArgs.length - 1; ++i)
	    {
	      FileInputFormat.addInputPath(retailJob, new Path(pathArgs[i]));
	    }
	    
	    FileOutputFormat.setOutputPath(retailJob, new Path(pathArgs[pathArgs.length - 1]));
	    System.exit(retailJob.waitForCompletion(true) ? 0 : 1);
	  }

}
