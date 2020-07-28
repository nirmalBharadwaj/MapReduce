package com.proejctgr.retail.stores;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class RetailReducer extends Reducer  <Text,FloatWritable,Text,FloatWritable> {
	
	
	MultipleOutputs<Text,FloatWritable> mos = null;
	
	public void setup(Context context ) {
		mos = new MultipleOutputs<Text,FloatWritable>(context);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
	
	public void reduce(Text k1, Iterable<FloatWritable> v1,Context context) throws IOException, InterruptedException {
		
		
		FloatWritable newV = new FloatWritable();
		Float count=0f;
		
		if(k1.toString().equals("bad-record-key")) {
			for(FloatWritable v: v1) {
				mos.write("BadRecords", v, new Text());
			}
		}
		
		else {
		for(FloatWritable v: v1) {
			count+= v.get();
		}
		
		newV.set(count);
		try {
			mos.write("ParsedRecords", k1, newV);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		}
		
	}
}
