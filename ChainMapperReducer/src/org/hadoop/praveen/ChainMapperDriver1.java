package org.hadoop.praveen;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChainMapperDriver1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			

		 Path inputPath = new Path("/user/cloudera/wc");
		 Path outputDir = new Path("/user/cloudera/output");

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Chain Job1");

		job.setJarByClass(ChainMapperDriver1.class);
		
		/* add multiple chained mappers to run for this job */
		ChainMapper.addMapper(job,
			      MyMapper1.class, /* Mapper to add to chain */
			      LongWritable.class, /* Mapper input Key */
			      Text.class, /* Mapper input value */
			      Text.class, /* Mapper output key*/
			      IntWritable.class, /* Mapper output value */
			      conf /* job configuration to use */
			      );

		/* set second mapper in the chain */
		ChainMapper.addMapper(job,
					  MyMapper2.class, /* Mapper to add to chain */
				      Text.class, /* Mapper input Key */
				      IntWritable.class, /* Mapper input value */
				      Text.class, /* Mapper output key*/
				      IntWritable.class, /* Mapper output value */
				      conf /* job configuration to use */
				      );
		
		/* set reducer for the chain */
		ChainReducer.setReducer(job,
					MyReducer.class, /* Reducer to add to chain */
					Text.class, /* Reducer input Key */
					IntWritable.class, /* Reducer input value */
					Text.class, /* Reducer output key*/
					IntWritable.class, /* Reducer output value */
					conf /* job configuration to use */
					);
			
		FileInputFormat.addInputPath(job, inputPath);
		
		FileOutputFormat.setOutputPath(job, new Path(outputDir + "_job1"));
		outputDir.getFileSystem(job.getConfiguration()).delete(new Path(outputDir + "_job1"), true);

		/* run first MR */
		if (!job.waitForCompletion(true))
		{
		    System.out.println("ERROR completing first job");
		    System.exit(1);
		}
		
		
		/* MR-2 */
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Chain Job2");

		job2.setJarByClass(ChainMapMain.class);

		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		/* Set input path: Read from putput of first MR */
		FileInputFormat.addInputPath(job2, new Path(outputDir + "_job1/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, outputDir);
		outputDir.getFileSystem(job2.getConfiguration()).delete(outputDir,true);

		/* run second MR */
		job2.waitForCompletion(true);
	}

}
