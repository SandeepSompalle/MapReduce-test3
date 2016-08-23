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
		String in = value.toString();
		
		System.out.println("Input is :: " + in);
//		if(in.startsWith("/"))
//		{
			// 7/9/2016, /data01/pi/pisecure, dhpimi02, 4/7/2015, 52, 41, 200, 600  
			String[] fields = in.split(",");
			
			System.out.println("Arraylength is :: " + fields.length);
			
			String path = 			
					fields[1] ;
			//Double size = Double.parseDouble(fields[1]);
			
			//String owner = fields[9];
			
			
	//Printing system date 
			
			Date sys_date = new Date();
			
			System.out.println("\n ");
			System.out.print(" System Date is :: ");
			
			System.out.println(new SimpleDateFormat("MM-dd-yyyy").format(sys_date));
			
			System.out.println( sys_date.toString());
			System.out.println("\n ");
			
			
			
	//arithmetic operations on system date
			
		String	Report_gen_date = fields[0].toString();
		
		System.out.println("\n ");
		
				
		
		System.out.printf("Report_gen_date :: %s",Report_gen_date );
		System.out.println("\n ");
		
        //String str = "7/9/2016";
		
		//converting date from mm/dd/yyyy to mm-dd-yyyy
		
		SimpleDateFormat date_in = new SimpleDateFormat("mm/dd/yyyy");
		
		SimpleDateFormat date_con = new SimpleDateFormat("mm-dd-yyyy");
		
		
		
		Date data= null;
		
		try {
			data = date_in.parse(Report_gen_date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String formattedTime = date_con.format(data);
		
		System.out.println(formattedTime);		
		
		//subtracting two string dates
		
		SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy", Locale.ENGLISH);
		
		//Date date_curr = null;
		
		
		DateTimeFormatter FMT = DateTimeFormat.forPattern("mm-dd-yyyy");
		
		final DateTime dt1 = new DateTime(FMT.parseDateTime(sys_date.toString()));
		final DateTime dt2 = new DateTime(FMT.parseDateTime(Report_gen_date));
		
		Days days = Days.daysBetween(dt1, dt2);
		
		System.out.print("Difference in days ::" + days +"");
		
		//int Diff_days = (int)days;
		
		System.out.println("\n ");
		
		
	/*	
		try {
			 
			Date date_curr = sdf.parse(sys_date.toString());
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//Date date_2 = null;
		try {
			Date date_2 = sdf.parse(Report_gen_date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		long differenceinMilliseconds = date_curr.getTime()- date_2.getTime();
		
		*/
		
				
		 //System.out.println(new SimpleDateFormat("MM-dd-yyyy").format(Report_gen_date));
		
		//System.out.println("\n ");
		
        //String value1 = sys_date - Report_gen_date;l
        
        
			
			
			
			context.write(new Text(path), new Text(

					", " + fields[0] + "," + fields[2] + "," + fields[3] + 
					"," + fields[4] + "," + fields[5] + ","  + fields[6] + 
					"," + fields[7]
					
					));
			
			
//		}
		}
	}
	


