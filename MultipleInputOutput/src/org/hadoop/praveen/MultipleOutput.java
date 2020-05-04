package org.hadoop.praveen;


import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleOutput {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Path inputPath = new Path("input");         // input file path
	    Path outputDir = new Path("output/");      // output directory path
	    
	    Configuration conf = new Configuration();
		Job job = new Job(conf, "Multi Output");
		
		//name of driver class
		job.setJarByClass(MultipleOutput.class);
		//name of mapper class
		job.setMapperClass(MultipleOutputMapper.class);
		//name of reducer class
		job.setReducerClass(MultipleOutputReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//add input path 
		FileInputFormat.addInputPath(job, inputPath);  

		// Set 2 outputs 
		MultipleOutputs.addNamedOutput(job,"HR", TextOutputFormat.class, Text.class, LongWritable.class );

		MultipleOutputs.addNamedOutput(job,"Accounts", TextOutputFormat.class, Text.class, LongWritable.class );

		FileOutputFormat.setOutputPath(job, outputDir);
		outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
