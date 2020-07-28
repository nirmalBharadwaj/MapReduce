package com.projects.file.join.adv;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileMapper extends Mapper <LongWritable,Text,Text,JoinWritable>{
	
	Text mkey = new Text();
	JoinWritable mvalue = new JoinWritable();
	String inputFileName;
	
	public void setup(Context context) {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		inputFileName = fileSplit.getPath().getName().toString();
		System.out.println(inputFileName);
	}
	
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
		
		try {
			context.write(mkey, new JoinWritable(val.toString(),inputFileName));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
