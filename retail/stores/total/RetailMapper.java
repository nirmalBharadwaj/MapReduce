package com.proejctgr.retail.stores.total;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RetailMapper extends Mapper<LongWritable,Text,Text,FloatWritable>  {
	
	
	public void map(LongWritable k1,Text v1,Context context) {
		
		String[] tokens = v1.toString().split("\t"); 
		FloatWritable newV= new FloatWritable(Float.parseFloat(tokens[4]));
		
		
		try {
			context.write(new Text("null"),newV);
		} catch (IOException | InterruptedException e) {
			
			e.printStackTrace();
		}
	}

}
