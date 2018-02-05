package cn.edu.blcu.nlp.probSpeed;

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

/*
 * 用于将left和right model的prob和back值计算出来
 * */
public class ProbSpeedDriver {
	public static void main(String[] args) {
		String input="";
		String output="";
		int tasks = 0;
		int isLzo=0;
		String lmFlag="";
		int gtmin=0;
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
			}else if(args[i].equals("-lmFlag")){
				lmFlag = args[++i];
				System.out.println("lmFlag--->"+lmFlag);
			}else if(args[i].equals("-gtmin")){
				gtmin=Integer.parseInt(args[++i]);
				System.out.println("gtmin--->"+gtmin);
			}else{
				System.out.println("there exists invalid parameters--->"+args[i]);
				break;
			}
		}
		
		try {
			
			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.set("lmFlag", lmFlag);
			conf.setInt("gtmin", gtmin);
			Job probJob = Job.getInstance(conf,lmFlag+" speed prob Job");
			System.out.println(probJob.getJobName()+" is running!!");
			probJob.setJarByClass(ProbSpeedDriver.class);
			probJob.setMapperClass(ProbSpeedMapper.class);
			probJob.setReducerClass(ProbSpeedReducer.class);
			probJob.setCombinerClass(ProbSpeedCombiner.class);
			probJob.setNumReduceTasks(tasks);
			
			probJob.setInputFormatClass(SequenceFileInputFormat.class);
			probJob.setMapOutputKeyClass(Text.class);
			probJob.setMapOutputValueClass(Text.class);
			probJob.setOutputKeyClass(Text.class);
			probJob.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(probJob, new Path(input));
			FileInputFormat.setInputDirRecursive(probJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(probJob, outputPath);
			probJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(probJob);
			}

			if (probJob.waitForCompletion(true)) {
				System.out.println(probJob.getJobName()+" Job successed");
			} else {
				System.out.println(probJob.getJobName()+" Job failed");
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
