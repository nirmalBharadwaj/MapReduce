package com.proejctgr.employeectc;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RetailMapper  extends Mapper<Object, Text, Text, Text>
{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{

		String[] tokens = value.toString().split("\t");
		String dept = tokens[4].toString();

		context.write(new Text(dept), value);
	}

}
