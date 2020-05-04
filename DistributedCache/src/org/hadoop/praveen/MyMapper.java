package org.hadoop.praveen;

import java.util.HashMap;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;


public class MyMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	
	private HashMap<String, Double> desg_map = new HashMap<String, Double>(); 
	@Override
	protected void setup(Context context) throws IOException {
		
		// Read data from the Distributed cache
		
		BufferedReader reader = null;
		Path[] localpath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		String record = "";
		
		for(Path path : localpath) {
			
			if(path.getName().toString().trim().equals("description.txt")) {
				reader = new BufferedReader(new FileReader(path.toString()));
				record = reader.readLine();
				while(record != null) {
					String data[] = record.split(",");                 //   [ {MGR} {2}]
					/* designation_code, increment_multiplier */
					desg_map.put(data[0].trim(), Double.parseDouble(data[1].trim()));
					record = reader.readLine();
				}
			}
		}

	}
	
	@Override
    protected void map(LongWritable key, Text value,  Context context)throws IOException, java.lang.InterruptedException
    {
	
		String line = value.toString();
		String[] words = line.split(",");                        

		 String designation= words[2] ;       
	    
	    double n = 1;
	    if (designation.toString().equalsIgnoreCase("manager"))
	    {
		  n = desg_map.get("MGR");       // n= 2
	    } 
	    else if(designation.toString().equalsIgnoreCase("developer"))
	    {
		  n = desg_map.get("DLP");        // n = 5
	    } 
	    else if(designation.toString().equalsIgnoreCase("hr"))
	    {
		  n = desg_map.get("HR");         // n = 6
	    } 
	    else
	    {
		System.out.println("Invalid designation");
	    }
	    
	    int currentSalary = Integer.parseInt(words[3].trim());
	    double increment = (n/100) * currentSalary;
	  
	    context.write(new Text(designation), new DoubleWritable(increment));
	}
	

}
