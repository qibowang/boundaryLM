package cn.edu.blcu.nlp.middleSegBack;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;


public class MiddleBackDriver{
	public static void main(String[] args) {
		String input="";
		String output="";
		int isLzo=0;
		int tasks=1;
		boolean parameterValid=false;
		for(int i=0;i<args.length;i++){
			if(args[i].equals("-input")){
				input=args[++i];
				System.out.println("input--->"+input);
			}else if(args[i].equals("-output")){
				output=args[++i];
				System.out.println("output--->"+output);
			}else if(args[i].equals("-isLzo")){
				isLzo=Integer.parseInt(args[++i]);
				System.out.println("isLzo--->"+isLzo);
			}else if(args[i].equals("-tasks")){
				tasks=Integer.parseInt(args[++i]);
				System.out.println("tasks--->"+tasks);
			}else{
				System.out.println("there exists invalid parameters--->"+args[i]);
				parameterValid=true;
			}
		}
		if(parameterValid){
			System.out.println("parameters invalid!!!!");
			System.exit(1);
		}
		
		
		try {
			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			
			Job middleBackJob = Job.getInstance(conf,"middle back job");
			
			middleBackJob.setJarByClass(MiddleBackDriver.class);
			middleBackJob.setMapperClass(MiddleBackMapper.class);
			middleBackJob.setReducerClass(MiddleBackReducer.class);
			middleBackJob.setNumReduceTasks(tasks);
			
			middleBackJob.setInputFormatClass(SequenceFileInputFormat.class);
			middleBackJob.setMapOutputKeyClass(Text.class);
			middleBackJob.setMapOutputValueClass(Text.class);
			middleBackJob.setOutputKeyClass(Text.class);
			middleBackJob.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(middleBackJob, new Path(input));
			FileInputFormat.setInputDirRecursive(middleBackJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(middleBackJob, outputPath);
			middleBackJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(middleBackJob);
			}

			if (middleBackJob.waitForCompletion(true)) {
				System.out.println(middleBackJob.getJobName()+" Job successed");
			} else {
				System.out.println(middleBackJob.getJobName()+" Job failed");
			}
			

			
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void setLzo(Job job) {
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
	}
	
}
