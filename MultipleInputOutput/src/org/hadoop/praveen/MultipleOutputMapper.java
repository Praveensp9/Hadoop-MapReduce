package org.hadoop.praveen;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleOutputMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	private Text empId = new Text();
	private Text empData = new Text();
	
	@Override
	protected void map(LongWritable key,Text value,Context con) throws IOException, java.lang.InterruptedException{
	
		String line = value.toString();        // DJPX255251,Arthur,HR,6397,2016 
		
		/* Split csv string */
		String[] words = line.split(","); 
		
		empId.set(words[0]);               // empId = DJPX255251
		
		empData.set(words[1] + "," + words[2] + "," + words[3]);
		
		con.write(empId, empData);
		//con.write(new Text(words[0]), new Text(words[1] + "," + words[2] + "," + words[3]));

	}

}
