package com.projects.sarlogs; 

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SarCpuMappper extends Mapper<LongWritable,Text,Text,FloatWritable>  {
	
	
	
	private FloatWritable percentval= new FloatWritable();
	private Text mkey = new Text();
	public void map(LongWritable key,Text value,Context context) {
		
		String tokens[] = value.toString().split(" ");
		
		String hostname = tokens[0];
		String date ="";
		String timeStamp="";
		
		// to find next non zero String 
		for(int i=1;i<tokens.length;i++) {
		
			if(tokens[i].length()>0) {
				timeStamp=tokens[i];
				break;
			}
		}
		
		date= timeStamp.split(",")[0];
		
		mkey.set(hostname+"\t"+date);
		percentval.set(100f- Float.parseFloat(tokens[tokens.length-1]));
		try {
			context.write(mkey,percentval);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		
	}

}

