package com.projects.fileJoins;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileMapper extends Mapper <LongWritable,Text,Text,Text>{
	
	Text mkey = new Text();
	Text mvalue = new Text();
	
	public void map(LongWritable key ,Text value,Context context) {
		
		String[] tokens = value.toString().split(",");
		StringBuffer val = new StringBuffer();
		
		for(int i=1;i<tokens.length;i++) {
			if(i==tokens.length-1)
				val.append(tokens[i]);
			else
			   val.append(tokens[i]+",");
		}
		
		mkey.set(tokens[0]);
		mvalue.set(val.toString());
		
		try {
			context.write(mkey, mvalue);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
