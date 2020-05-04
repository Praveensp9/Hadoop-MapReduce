package org.hadoop.praveen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WritableMapper extends Mapper<LongWritable,Text,MyWritable,IntWritable>{
	
	private final IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text val, Context c) throws IOException, InterruptedException{
			String line = val.toString();
			String[] words = line.split(",");
			for(String word:words) {
				c.write(new MyWritable(word), one);
			}
	}

}
