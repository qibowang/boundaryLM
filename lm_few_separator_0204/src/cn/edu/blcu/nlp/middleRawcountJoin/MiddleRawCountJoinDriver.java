package cn.edu.blcu.nlp.middleRawcountJoin;

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

public class MiddleRawCountJoinDriver {
	public static void main(String[] args) {
		String input="";
		String output="";
		int isLzo=0;
		int tasks=0;
		int order=3;
		for(int i=0;i<args.length;i++){
			if(args[i].equals("-input")){
				input = args[++i];
				System.out.println("input--->"+input);
			}else if(args[i].equals("-output")){
				output = args[++i];
				System.out.println("output--->"+output);
			}else if(args[i].equals("-tasks")){
				tasks= Integer.parseInt(args[++i]);
				System.out.println("tasks--->"+tasks);
			}else if(args[i].equals("-isLzo")){
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo--->"+isLzo);
				
			}else if(args[i].equals("-order")){
				order = Integer.parseInt(args[++i]);
				System.out.println("order--->"+order);
			}else{
				System.out.println("there exists invalid parameters--->"+args[i]);
				break;
			}
		}
		
		
		try {
			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.setInt("order", order);
			Job joinJob = Job.getInstance(conf, "middle rawcount join job");
			System.out.println(joinJob.getJobName()+" is running!");
			
			joinJob.setJarByClass(MiddleRawCountJoinDriver.class);
			joinJob.setMapperClass(MiddleRawCountJoinMapper.class);
			joinJob.setReducerClass(MiddleRawCountJoinReducer.class);
			joinJob.setNumReduceTasks(tasks);
			
			joinJob.setInputFormatClass(SequenceFileInputFormat.class);
			joinJob.setMapOutputKeyClass(Text.class);
			joinJob.setMapOutputValueClass(Text.class);
			joinJob.setOutputKeyClass(Text.class);
			joinJob.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(joinJob, new Path(input));
			FileInputFormat.setInputDirRecursive(joinJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(joinJob, outputPath);
			joinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(joinJob);
			}

			if (joinJob.waitForCompletion(true)) {
				System.out.println(joinJob.getJobName()+" Job successed");
			} else {
				System.out.println(joinJob.getJobName()+" Job failed");
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
