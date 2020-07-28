package com.proejctgr.retail.stores;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RetailMapper extends Mapper<LongWritable,Text,Text,FloatWritable>  {
	
	
	public void map(LongWritable k1,Text v1,Context context) {
		
		String[] tokens = v1.toString().split("\t"); 
		Text newk = new Text(tokens[2]);
		FloatWritable newV= new FloatWritable(Float.parseFloat(tokens[4]));
		
		
		try {
			context.write(newk,newV);
		} catch (IOException | InterruptedException e) {
			
			e.printStackTrace();
		}
	}

}
