package com.proejctgr.NumberCount;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;

public class NumberMapper extends Mapper<LongWritable,Text,LongWritable,FloatWritable> {
	
	LongWritable newKey = new LongWritable();
	FloatWritable newValue = new FloatWritable();
	
	public void map(LongWritable k1,Text v1,Context context) {
		
		Long tempk = new Long(1) ;
		Float tempV=Float.valueOf(v1.toString());
		
		newKey.set(tempk);
		newValue.set(tempV);
		
		try {
			context.write(newKey, newValue);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
