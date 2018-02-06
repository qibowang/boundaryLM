package cn.edu.blcu.nlp.middleLM;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;



public class MiddleSegLMDriver {
	public static void main(String[] args) {
		String probPath="";
		String backPath="";
		String lmPath="";
		int isLzo=0;
		int tasks=1;
		boolean parameterValid=false;
		int parameterNum = args.length;
		for(int i=0;i<parameterNum;i++){
			if(args[i].equals("-prob")){
				probPath=args[++i];
				System.out.println("probPath--->"+probPath);
			}else if(args[i].equals("-back")){
				backPath=args[++i];
				System.out.println("backPath--->"+backPath);
			}else if(args[i].equals("-lm")){
				lmPath=args[++i];
				System.out.println("lmPath--->"+lmPath);
			}else if(args[i].equals("-isLzo")){
				isLzo=Integer.parseInt(args[++i]);
				System.out.println("isLzo--->"+isLzo);
			}else if(args[i].equals("-tasks")){
				tasks=Integer.parseInt(args[++i]);
				System.out.println("tasks-->"+tasks);
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
			
			Job lmJob = Job.getInstance(conf,"middle LM job");
			
			lmJob.setJarByClass(MiddleSegLMDriver.class);
			lmJob.setReducerClass(MiddleSegLMReducer.class);
			lmJob.setSortComparatorClass(MyComparator.class);
			lmJob.setNumReduceTasks(tasks);
			
			lmJob.setMapOutputKeyClass(Text.class);
			lmJob.setMapOutputValueClass(Text.class);
			lmJob.setOutputKeyClass(Text.class);
			lmJob.setOutputValueClass(Text.class);
			
			MultipleInputs.addInputPath(lmJob, new Path(probPath), SequenceFileInputFormat.class,MiddleSegLMMapperProb.class);
			MultipleInputs.addInputPath(lmJob, new Path(backPath), SequenceFileInputFormat.class,MiddleSegLMMapperBack.class);
			lmJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputDirRecursive(lmJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(lmPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(lmJob, outputPath);
			lmJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(lmJob);
			}

			if (lmJob.waitForCompletion(true)) {
				System.out.println(lmJob.getJobName()+" Job successed");
			} else {
				System.out.println(lmJob.getJobName()+" Job failed");
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
