package cn.edu.blcu.nlp.checkrawcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class CheckDriver {
	public static void main(String[] args) {
		String input = args[0];
		String output = args[1];
		Configuration conf = new Configuration();
		Job checkJob;
		try {
			checkJob = Job.getInstance(conf," check Job");
			System.out.println(checkJob.getJobName()+" is running!!");
			checkJob.setJarByClass(CheckDriver.class);
			checkJob.setMapperClass(CheckMapper.class);
			checkJob.setCombinerClass(CheckReducer.class);
			checkJob.setReducerClass(CheckReducer.class);
			checkJob.setNumReduceTasks(1);
			
			checkJob.setInputFormatClass(SequenceFileInputFormat.class);
			checkJob.setMapOutputKeyClass(Text.class);
			checkJob.setMapOutputValueClass(IntWritable.class);
			checkJob.setOutputKeyClass(Text.class);
			checkJob.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(checkJob, new Path(input));
			FileInputFormat.setInputDirRecursive(checkJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(checkJob, outputPath);
			checkJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			

			if (checkJob.waitForCompletion(true)) {
				System.out.println(checkJob.getJobName()+" Job successed");
			} else {
				System.out.println(checkJob.getJobName()+" Job failed");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		
	}
}
