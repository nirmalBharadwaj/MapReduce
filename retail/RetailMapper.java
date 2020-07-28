package com.proejctgr.retail;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class RetailMapper extends Mapper<LongWritable,Text,Text,FloatWritable>  {
	
	private Logger logger = Logger.getLogger(RetailMapper.class);
	
	public void map(LongWritable k1,Text v1,Context context) {
		
		String[] tokens = v1.toString().split("\t"); 
		Text newk = new Text(tokens[3]);
		FloatWritable newV= new FloatWritable(Float.parseFloat(tokens[4]));
		
		logger.info("Key:" + tokens[3] + "Value: "+ tokens[4] );
		
		try {
			context.write(newk,newV);
		} catch (IOException | InterruptedException e) {
			
			e.printStackTrace();
		}
	}

}
