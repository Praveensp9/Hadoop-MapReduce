package org.hadoop.praveen;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WritableReducer extends Reducer<MyWritable, IntWritable, MyWritable, IntWritable>{
	
	 	@Override
	    protected void reduce(MyWritable key, Iterable<IntWritable> values, Context c)throws IOException,	java.lang.InterruptedException
	    {
		int sum = 0;
		Iterator<IntWritable> valuesIter = values.iterator();
		while (valuesIter.hasNext()){
		    sum += valuesIter.next().get();
		}
		c.write(key, new IntWritable(sum));
	    }


}
