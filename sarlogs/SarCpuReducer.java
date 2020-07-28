package com.projects.sarlogs; 

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SarCpuReducer  extends Reducer<Text,FloatWritable,Text,Text> {

	Text result = new Text();

	public void reduce(Text key ,Iterable<FloatWritable> value,Context context) {
		
		int count =0;
		float sum=0;
		
		for (FloatWritable v : value) {
			count++;
			sum+=v.get();
		}
		
		float aggregate= sum/count;
		result.set(aggregate+"");
		try {
			context.write(key,result );
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
