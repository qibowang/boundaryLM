package cn.edu.blcu.nlp.middleMle;

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



public class MiddleMleDriver {
	public static void main(String[] args) {
		String input="";
		String output="";
		int tasks = 0;
		int isLzo=0;
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
				System.out.println("there exists invalid parameters-->"+args[i]);
				break;
			}
		}
		
		try {
			
			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.setInt("order", order);
			Job middleMleJob = Job.getInstance(conf,"mleJob");
			
			middleMleJob.setJarByClass(MiddleMleDriver.class);
			middleMleJob.setMapperClass(MiddleMleMapper.class);
			middleMleJob.setReducerClass(MiddleMleReducer.class);
			middleMleJob.setNumReduceTasks(tasks);
			
			middleMleJob.setInputFormatClass(SequenceFileInputFormat.class);
			middleMleJob.setMapOutputKeyClass(Text.class);
			middleMleJob.setMapOutputValueClass(Text.class);
			middleMleJob.setOutputKeyClass(Text.class);
			middleMleJob.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(middleMleJob, new Path(input));
			FileInputFormat.setInputDirRecursive(middleMleJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(middleMleJob, outputPath);
			middleMleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(middleMleJob);
			}

			if (middleMleJob.waitForCompletion(true)) {
				System.out.println("mle Job successed");
			} else {
				System.out.println("mle Job failed");
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
