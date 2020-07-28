package com.proejctgr.retail.stores.total;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class RetailReducer extends Reducer  <Text,FloatWritable,Text,FloatWritable> {
	
	

	
	public void reduce(Text k1, Iterable<FloatWritable> v1,Context context) throws IOException, InterruptedException {
		
		
		FloatWritable newV = new FloatWritable();
		Text newk = new Text();
		Float count=0f;
	    int total=0;	
	
		for(FloatWritable v: v1) {
			total+=1;
			count+= v.get();
		}
		
		newV.set(count);
		newk.set(String.valueOf(total));
		
		try {
			context.write(newk, newV);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
		
	}
}
