package cn.edu.blcu.nlp.mleright;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;


public class MLEDriver {
	public static void main(String[] args) {
		String input="";
		String output="";
		String flag="";
		int tasks=1;
		int isLzo=0;
		for(int i=0;i<args.length;i++){
			if(args[i].equals("-flag")){
				flag=args[++i];
				System.out.println("flag---->"+flag);
			}else if(args[i].equals("-input")){
				input=args[++i];
				System.out.println("input--->"+input);
			}else if(args[i].equals("-output")){
				output=args[++i];
				System.out.println("output--->"+output);
			}else if(args[i].equals("-task")){
				tasks= Integer.parseInt(args[++i]);
				System.out.println("tasks--->"+tasks);
			}else if(args[i].equals("-isLzo")){
				isLzo=Integer.parseInt(args[++i]);
				System.out.println("isLzo--->"+isLzo);
			}else{
				System.out.println("there exists invalid parameters-->"+args[i]);
				break;
			}
		}
		
		
		
		try {
			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.set("flag", flag);
			Job job = Job.getInstance(conf,"middle mle-->"+flag);
			System.out.println(job.getJobName()+" is running!");
			
			job.setJarByClass(MLEDriver.class);
			job.setMapperClass(MLEMapper.class);
			job.setReducerClass(MLEReducer.class);
			job.setNumReduceTasks(tasks);
			
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(input));
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
				System.out.println(job.getJobName()+" job successed!");
			} else {
				System.out.println(job.getJobName()+" job failed!");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void setLzo(Job job) {
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
	}
	
}
