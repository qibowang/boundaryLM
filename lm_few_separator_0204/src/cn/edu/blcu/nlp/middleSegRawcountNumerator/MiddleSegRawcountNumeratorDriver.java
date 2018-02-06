package cn.edu.blcu.nlp.middleSegRawcountNumerator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;

public class MiddleSegRawcountNumeratorDriver {
	public static void main(String[] args) {
		int startOrder=0;
		int endOrder=3;
		int tasks=1;
		int isLzo=0;
		String input="";
		String output="";
		boolean parameterValid=false;
		int parameterNum = args.length;
		for(int i=0;i<parameterNum;i++){
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
			}else if(args[i].equals("-startOrder")){
				startOrder=Integer.parseInt(args[++i]);
				System.out.println("startOrder--->"+startOrder);
			}else if(args[i].equals("-endOrder")){
				endOrder=Integer.parseInt(args[++i]);
				System.out.println("endOrder--->"+endOrder);
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
			conf.setInt("startOrder", startOrder);
			conf.setInt("endOrder", endOrder);
			
			Job job = Job.getInstance(conf,"middleSegRawcountNumerator job");
			System.out.println(job.getJobName()+" is running!");
			job.setJarByClass(MiddleSegRawcountNumeratorDriver.class);
			
			job.setMapperClass(MiddleSegRawcountNumeratorMapper.class);
			job.setCombinerClass(MiddleSegRawcountNumeratorCombiner.class);
			job.setReducerClass(MiddleSegRawcountNumeratorReducer.class);
			job.setPartitionerClass(MiddleSegRawcountNumeratorPartitioner.class);
			job.setNumReduceTasks(tasks);
			
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			Path inputPath = new Path(input);
			FileInputFormat.addInputPath(job, inputPath);
			FileInputFormat.setInputDirRecursive(job, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(job);
			}

			if (job.waitForCompletion(true)) {
				System.out.println(job.getJobName()+" Job successed");
			} else {
				System.out.println(job.getJobName()+" Job failed");
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void setLzo(Job job) {
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
	}
}
