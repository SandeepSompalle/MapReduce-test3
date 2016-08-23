package com.MapReuce.HdfsInventory;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

public class DriverInventory extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		int exitCode = ToolRunner.run(new DriverInventory(), args);
	    System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		if (args.length != 2) {
		      System.out.printf(
		          "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
		              .getSimpleName());
		      ToolRunner.printGenericCommandUsage(System.out);
		      return -1;
		    }
		
		
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(DriverInventory.class);
	    job.setJobName(this.getClass().getName());
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputValueClass(TextOutputFormat.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class); 
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    
	    job.setMapperClass(MapperInventory.class);
	    //job.setReducerClass(ReducerInventory.class);
	    
//	    job.setMapperClass(MapperInventory.class);
//	    job.setReducerClass(ReducerInventory.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
				
				
		//  job.setNumReduceTasks(1);

	    if (job.waitForCompletion(true)) {
	      return 0;
	    }
		
		return 1;
	}

	
	
}
