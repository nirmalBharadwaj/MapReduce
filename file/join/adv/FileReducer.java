package com.projects.file.join.adv;


import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileReducer extends Reducer<Text,JoinWritable,Text,Text>{
	
	Text rvalue = new Text();

	public void reduce(Text key,Iterable<JoinWritable> value, Context context ) throws IOException, InterruptedException {
		
		StringBuffer val_1 = new StringBuffer();
		StringBuffer val_2 = new StringBuffer();
		
		for (JoinWritable v:value) {
			
			if(v.getFileName().equals("empdept.txt")) {
				
				val_1.append(v.getValue());
			}
			
			else if (v.getFileName().equals("empname.txt")){
				val_2.append(v.getValue());
			}
			
			else {
				val_2.append(v.getValue());	
			}
			
		}
		val_2.append("\t"+val_1);
		
		rvalue.set(val_2.toString());
		context.write(key,rvalue);
	}
	
	
    
}
