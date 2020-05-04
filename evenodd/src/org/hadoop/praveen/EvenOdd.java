package org.hadoop.praveen;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EvenOdd {

	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException{
		
		Path input = new Path("oddeven.txt");
		Path output = new Path("output");
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"OddEven");
		
		job.setJarByClass(EvenOdd.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setCombinerClass(CombinerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(job.getConfiguration()).delete(output,true);
		
		boolean bool = job.waitForCompletion(true);
		if(bool)
			System.out.println("Successful :"+0);
		else
			System.out.println("Failed :"+1);
		

	}

}