package cn.edu.blcu.nlp.probJoinBack;

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

import cn.edu.blcu.nlp.middleSegProbJoinBack.MyComparator;

public class ProbJoinBackDriver {
	public static void main(String[] args) {
		String input="";
		String output="";
		String lmFlag="";
		int tasks=0;
		int isLzo=0;
		int index=0;
		String inputPaths[]=new String[10];
		for(int i=0;i<args.length;i++){
			if(args[i].equals("-input")){
				input = args[++i];
				if(index<inputPaths.length){
					inputPaths[index++]=input;
				}else{
					System.out.println("input paths are more than 10 please build the jar file again");
				}
				System.out.println("inputPath--->" + input);
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
				lmFlag=args[++i];
				System.out.println("lmFlag--->"+lmFlag);
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
			Job probJoinProbJob = Job.getInstance(conf, "prob join back job");
			
			probJoinProbJob.setJarByClass(ProbJoinBackDriver.class);
			probJoinProbJob.setMapperClass(ProbJoinBackMapper.class);
			probJoinProbJob.setReducerClass(ProbJoinBackReducer.class);
			probJoinProbJob.setSortComparatorClass(MyComparator.class);
			probJoinProbJob.setNumReduceTasks(tasks);
			
			probJoinProbJob.setInputFormatClass(SequenceFileInputFormat.class);
			probJoinProbJob.setMapOutputKeyClass(Text.class);
			probJoinProbJob.setMapOutputValueClass(Text.class);
			probJoinProbJob.setOutputKeyClass(Text.class);
			probJoinProbJob.setOutputValueClass(Text.class);
			
			for(String inputPath:inputPaths){
				if(inputPath!=null){
					FileInputFormat.addInputPath(probJoinProbJob, new Path(inputPath));
				}
			}
			FileInputFormat.setInputDirRecursive(probJoinProbJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(probJoinProbJob, outputPath);
			probJoinProbJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(probJoinProbJob);
			}
			System.out.println(probJoinProbJob.getJobName()+" job is running!!!");
			if (probJoinProbJob.waitForCompletion(true)) {
				System.out.println(probJoinProbJob.getJobName()+" job successed");
			} else {
				System.out.println(probJoinProbJob.getJobName()+" job failed");
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
