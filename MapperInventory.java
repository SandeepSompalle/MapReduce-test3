package com.MapReuce.HdfsInventory;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class MapperInventory extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println(value);
		System.out.println("");
		String in = value.toString().replaceAll("\\s+","");
		System.out.println(in);
		
		String[] fields = in.split(",");
		
		
		System.out.println();
		
		//String path = null;
		String path = fields[1];
		
		 		
		for(int i=0; i< fields.length; i++)
		{
			System.out.println(fields[i]);
			

		    System.out.println("");
	    }	
			
	
		context.write(new Text(path), new Text(fields[0] + "," + fields[2] + "," + fields[3] + 
				"," + fields[4] + "," + fields[5] + ","  + fields[6] + 
				"," + fields[7]
				
				));
		
		
		}
	}
	

