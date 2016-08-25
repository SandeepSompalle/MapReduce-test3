package com.MapReuce.HdfsInventory;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerInventory extends Reducer<Text, Text, NullWritable, Text> {
	
	/*
	protected void setup(Context context) throws IOException, InterruptedException {
		
		String Str = "Directory Path, Directory User, Directory Creation Date, Previous Num of Directories, current num of Directories, Previous Dir Size in GB, Current Dir Size in Gb, dir_diff, file_diff, size_diff  ";
				
	    context.write(NullWritable.get(), new Text(Str));   
	    //context.write(NullWritable.get(), new Text(Str));   
	  }
	*/
	//context.write("a, " + "b, ");
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		
		System.out.println("");
		System.out.println("executing reducer code");
		System.out.println("");
				
		String[] olddata = null;
		String[] newdata = null;
		
		
		int i =0;
		//values.iterator().hasNext()
		
		String[] temp = new String[2];
		int z = 0;
		
		Iterator<Text> it = values.iterator();
		
		while(values.iterator().hasNext()){
			
			temp[z] = it.next().toString();
			z++;
		}
		
		
		
		if(temp[0] != null && temp[1] !=  null)
		{
			String data1[] = temp[0].split(",");
			String data2[] = temp[1].split(",");
			DateFormat formatter = new SimpleDateFormat("mm/dd/yyyy");
		
			Long diff = 0L;
			try {
				Long ts1 = formatter.parse(data1[0]).getTime();
				Long ts2 = formatter.parse(data2[0]).getTime();
				diff = ts1 - ts2;
			
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			if(diff >= 0)
			{
				olddata = data2;
				newdata = data1;
			}
			else
			{
				olddata = data1;
				newdata = data2;
			}
	
			Float oldsize = Float.parseFloat(olddata[6].replaceAll("/^[0-9]/", ""));
			Float newsize = Float.parseFloat(newdata[6].replaceAll("/^[0-9]/", ""));
			
			Float size_diff = newsize - oldsize;
			
			
			
			if(size_diff >= 0)
			{
				
				String res = Arrays.toString(newdata);
				System.out.print("REsult string is  :: " + res);
				System.out.println("Data2 :: " + res.substring(1, newdata.length) + " with new size " + newsize + 
						"Size increased by " + size_diff + "GB");
				
				//context.write(NullWritable.get(), new Text(key + "," + 
					//	Arrays.toString(newdata).substring(1, newdata.length) + 
						//",Size increased by " + size_diff + "GB"));
				//System.out.println("Size increased by " + size_diff + "GB");				
			}
			else
			{
				System.out.println("Data1 :: " + Arrays.toString(olddata) + " with old size " + oldsize +
						"Size Decreased by " + size_diff + "GB");				
			}
			
			
			//System.out.println("Data1 :: " + Arrays.toString(olddata) + " with old size " + oldsize);
			
		
		}
		
		else
		{
			if(temp[0] == null && temp[1] != null)
			{
				System.out.println("New Directory :: " + temp[1]);
			}
			else if(temp[0] != null && temp[1] == null)
			{
				System.out.println("New Directory :: " + temp[0]);
			}
		}
		/*
		for(Text value : values){			
			
			System.out.println("Iteration "+ i + "::" + value.toString()  + "");
			
			
			
			String[] tmp = value.toString().split(",");
			
			
			if (i == 0)
			{  
				System.out.print("data1 ::");
				
				data1 = tmp;
				
				for(int j=0; j < tmp.length; j++){
					
					System.out.print(tmp[j] + ",");
					
				}
				
			}else{
				
				data2 = tmp;
				
				System.out.print("data2 ::");	
				
            for(int j=0; j < tmp.length; j++){
					
					System.out.print(tmp[j] + ",");
				}
			}
			
			
			//System.out.print(data1[0]-data2[0]);
			


               String str_date1=data1[0];
               DateFormat formatter ; 
               Date date1 = null ; 
               formatter = new SimpleDateFormat("mm/dd/yyyy");
               try {
				date1 = formatter.parse(str_date1);
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
               

               String str_date2=data1[0];
               DateFormat formatter1 ; 
               Date date2 = null; 
               formatter1 = new SimpleDateFormat("mm/dd/yyyy");
               try {
				date2 = formatter1.parse(str_date2);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
               
               Long ts1 = date1.getTime();
               Long ts2 = date2.getTime();
               
               Long diff = ts1-ts2;

               String[] newvalue = null;
               
               String[] oldvalue = null;
               
			if(diff > 0){

				newvalue = data1;
				oldvalue = data2;
				
			}else{
				newvalue = data2;
				oldvalue = data1;
				
			}
			
			System.out.println("Pringting New Value");
			
			for(int k=0; k < newvalue.length; k++){
				
				System.out.print(newvalue[k] + ",");
				
			}
			
			System.out.println("");
			
            System.out.println("Pringting old Value");
			
			for(int l=0; l < oldvalue.length; l++){
				
				System.out.print(oldvalue[l] + ",");
				
			}
			
			
			
			
			
			System.out.println("");
			
			i++;
						
		}
		
		
		
		
		/*
		
		
		
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
		
		*/

    }
}
