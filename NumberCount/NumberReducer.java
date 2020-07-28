package com.proejctgr.NumberCount;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NumberReducer extends Reducer <LongWritable,FloatWritable,LongWritable,FloatWritable>{
	
	
	FloatWritable newValue = new FloatWritable();

	public void reduce(LongWritable k1, Iterable<FloatWritable> v1,Context context) {
		
		Float tempV = new Float(0);
		
		for(FloatWritable v : v1) {
			tempV += v.get();
		}
		
		newValue.set(tempV);
		
		try {
			context.write(k1,newValue);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
