package org.hadoop.praveen;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.net.URISyntaxException;

public class DCache {

	public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {

		Path inputPath = new Path("emp.txt");
		Path outputDir = new Path("output");
		
		Configuration conf = new Configuration();
		
		DistributedCache.addCacheFile(new URI("description.txt"), conf);
		
		Job job = new Job(conf, "Salary Increment");

		//name of driver class
		job.setJarByClass(DCache.class);
		//name of Mapper class
		job.setMapperClass(MyMapper.class);
		//name of Reducer class
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
	    FileInputFormat.addInputPath(job, inputPath);        
		FileOutputFormat.setOutputPath(job, outputDir);
		
		outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

		job.waitForCompletion(true);

	}

}
