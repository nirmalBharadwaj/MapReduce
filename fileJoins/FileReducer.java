package com.projects.fileJoins;


import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileReducer extends Reducer<Text,Text,Text,Text>{
	
	Text rvalue = new Text();
	public void reduce(Text key,Iterable<Text> value, Context context ) {
		
		StringBuffer val = new StringBuffer();
		for (Text v:value) {
			val.append(v.toString()+" ");
		}
		rvalue.set(val.toString());
		try {
			context.write(key, rvalue);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
    
}
