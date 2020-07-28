package com.proejctgr.retail;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RetailReducer extends Reducer  <Text,FloatWritable,Text,FloatWritable> {

	
	public void reduce(Text k1, Iterable<FloatWritable> v1,Context context) {
		
		
		FloatWritable newV = new FloatWritable();
		Float count=0f;
		
		for(FloatWritable v: v1) {
			count+= v.get();
		}
		
		newV.set(count);
		try {
			context.write(k1,newV);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
