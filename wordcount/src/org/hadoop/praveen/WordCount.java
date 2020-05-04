package org.hadoop.praveen;

import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable; // Int wrapper for hadoop
import org.apache.hadoop.io.LongWritable; // Long wrapper for hadoop
import org.apache.hadoop.io.Text; // String wrapper for hadoop
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] file = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Path input = new Path(file[0]);
		Path output = new Path(file[1]);
		
		Job job = new Job(conf,"wordcount");
		
		job.setJarByClass(WordCount.class);
		
		// Mapper
		job.setMapperClass(MapperClass.class);
		
		// Reducer
		job.setReducerClass(Reducer.class);
		
		// Combiner
		job.setCombinerClass(CombinerClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		
		boolean res = job.waitForCompletion(true);
		if(res)
			System.out.println("Successful :"+0);
		else
			System.out.println("Failed :"+1);
	}
	
	public static class MapperClass extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString();
			String[] words = line.split(",");
			for(String word:words) {
				Text outputKey = new Text(word.toUpperCase().trim());
				IntWritable outputValue = new IntWritable(1);
				context.write(outputKey,outputValue);
			}
		}
	}

	public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text word, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			
			int sum = 0;
			for(IntWritable value : values) {
				sum = sum + value.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}

	public static class CombinerClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text word,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			
			int sum=0;
			for(IntWritable value:values) {
				sum = sum + value.get();
			}
			context.write(word,new IntWritable(sum));
		}

	}
}
