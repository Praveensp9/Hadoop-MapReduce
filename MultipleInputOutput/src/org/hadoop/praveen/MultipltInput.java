package org.hadoop.praveen;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipltInput {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputpath1 = new Path("word1.txt");
		Path inputpath2 = new Path("word2.txt");
		
		Path output = new Path("output");
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		Job job = new Job(conf, "Multiple input example");
		
		job.setJarByClass(MultipltInput.class);
		
		job.setMapperClass(MyMapper1.class);
		job.setMapperClass(MyMapper2.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// adding the parameters for first input file
		MultipleInputs.addInputPath(job, inputpath1, TextInputFormat.class,MyMapper1.class);
		
		// adding the parameters for second input file
		MultipleInputs.addInputPath(job, inputpath2, KeyValueTextInputFormat.class,MyMapper2.class);

		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(job.getConfiguration()).delete(output,true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
	}

}
