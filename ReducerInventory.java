package com.MapReuce.HdfsInventory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerInventory extends Reducer<Text, Text, NullWritable, Text> {
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		
		String oldstr = null;
		String newstr = null;
		
		for(Text in : values)
		{
			String tmp = in.toString();
			int len = tmp.split(",").length;
			System.out.println(tmp + " :::: " + len);
			if(len > 1)
			{
				newstr = tmp;
			}
			else
			{
				oldstr = tmp;
			}
			
		}
		
		Double oldsize = Double.parseDouble(oldstr);
		
		Double newsize = Double.parseDouble(newstr.split(",")[0]);
		
		Double sizediff = newsize - oldsize;
		
		//context.write(NullWritable.get(), new Text(key + "," + newstr + "," + sizediff));
		
		if(sizediff>0)
		{	
		
		    context.write(NullWritable.get(), new Text(key + "	" + newstr + "	" + "Size increased by" +sizediff+ "GB"));
		
		}else{
			
			context.write(NullWritable.get(), new Text(key + "	" + newstr + "	" + "Size decreased by" +sizediff+ "GB"));
		}
		
		

    }
}
