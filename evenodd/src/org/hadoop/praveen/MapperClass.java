package org.hadoop.praveen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	public static Integer parse(String str) {
		try {
			return Integer.parseInt(str);
		}
		catch(Exception e) {
			return null;
		}
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
		
		String[] line = value.toString().split(",");
		
		for(String str:line) {
			
//			if(value.matches("\\d+") {
//			    Integer.parseInt(value);
//			}
			Integer num = parse(str);
			if(num != null) {
			
				if(num%2 == 0) {
					context.write(new Text("Even"), new IntWritable(num));
				}
				else {
					context.write(new Text("ODD"), new IntWritable(num));
				}
			}
		}
		
	}
	
}
