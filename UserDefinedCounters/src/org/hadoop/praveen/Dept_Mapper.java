package org.hadoop.praveen;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


// STATIC Counters defined below
enum LOCATION
{
	TOTAL,BANGALORE,CHENNAI,HYDERBAD
}

public class Dept_Mapper extends Mapper<LongWritable,Text,Text,Text>{
	
	private Text storelocation  = new Text();
	private Text data = new Text();
	
	@Override
	protected void map(LongWritable key, Text value,Context con) throws IOException, InterruptedException {
		
		/* total records processed by all mappers */
		// Static Counter
		con.getCounter(LOCATION.TOTAL).increment(1);
		
		String line = value.toString();
		String[] words = line.split(",");
		
		if (words[4].equalsIgnoreCase("bangalore"))
		{
			con.getCounter(LOCATION.BANGALORE).increment(1);
		}
		else if (words[4].equalsIgnoreCase("chennai"))
		{
		    con.getCounter(LOCATION.CHENNAI).increment(1);
		}
		else if (words[4].equalsIgnoreCase("hyderabad"))
		{
		    con.getCounter(LOCATION.HYDERBAD).increment(1);
		}
		else
			throw new RuntimeException("No Such City");
		
		
		// DYNAMIC Counters are defined Below.
		
		/* sale counters  - dynamic */
		int salesCount = Integer.parseInt(words[3]);
		if (salesCount < 10)
			con.getCounter("SALES", "LOW_SALES").increment(1);
		
		int price = Integer.parseInt(words[2]);
		if ((salesCount*price) > 500)
			con.getCounter("SALES","HIGH_REVENUE").increment(1);
		
		storelocation.set(words[4]);               // storeLocation = Hyderabad
		/* product_price, no.of sales */
		data.set(words[2] + "," + words[3]);        // data = 39,3

		con.write(storelocation, data);	
		
	}

}
