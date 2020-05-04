package org.hadoop.praveen;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper1 extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	private static final IntWritable one = new IntWritable(1);
	
	@Override
		protected void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {
		/* First Mapper reads in each line, split it into words and emit every word */
		String[] words = value.toString().split(",");

		for (String word : words)
		{
		    con.write(new Text(word), one);
		}
	}
}
