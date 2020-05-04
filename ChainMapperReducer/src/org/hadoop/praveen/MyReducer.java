package org.hadoop.praveen;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context c)throws IOException, java.lang.InterruptedException
    {

	int totalWordFrequency = 0;
	for (IntWritable count : values)
	{
	    totalWordFrequency += count.get();
	}
	/* emit total frequency for each word */
	c.write(key, new IntWritable(totalWordFrequency));
    }
}