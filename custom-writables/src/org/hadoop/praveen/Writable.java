package org.hadoop.praveen;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Writable {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputPath = new Path("hdfs://localhost:9000/user/jivesh/writable");     // input file path
    	Path outputDir = new Path("hdfs://localhost:9000/user/jivesh/output/");      // output directory path
		
     	Configuration conf = new Configuration();
	    Job job = new Job(conf, "Word Count Writable");

		  //name of driver class
	    job.setJarByClass(Writable.class);
	     //name of mapper class
		job.setMapperClass(WritableMapper.class);
		//name of reducer class
		job.setReducerClass(WritableReducer.class);
	
		job.setMapOutputKeyClass(MyWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(MyWritable.class);
		job.setOutputValueClass(IntWritable.class);
	
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
