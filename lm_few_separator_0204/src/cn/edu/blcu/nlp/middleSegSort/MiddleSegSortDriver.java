package cn.edu.blcu.nlp.middleSegSort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.hadoop.compression.lzo.LzoCodec;

public class MiddleSegSortDriver {
	public static void main(String[] args) {
		String input = "";
		String output = "";
		String lmFlag="middle";
		boolean parameterValid=false;
		int parameters=args.length;
		for (int i = 0; i < parameters; i++) {
			if (args[i].equals("-input")) {
				input = args[++i];
				System.out.println("input--->" + input);
			} else if (args[i].equals("-output")) {
				output = args[++i];
				System.out.println("output--->" + output);
			}  else if(args[i].equals("-lmFlag")){
				lmFlag=args[++i];
				System.out.println("lmFlag--->"+lmFlag);
			}else {
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

			Job sortJob = Job.getInstance(conf, lmFlag+" sort Job");
			System.out.println(sortJob.getJobName()+" is running!");
			sortJob.setJarByClass(MiddleSegSortDriver.class);
			sortJob.setMapperClass(MiddleSegSortMapper.class);
			sortJob.setReducerClass(MiddleSegSortReducer.class);
			sortJob.setSortComparatorClass(MyComparator.class);
			sortJob.setNumReduceTasks(1);

			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			sortJob.setMapOutputKeyClass(Text.class);
			sortJob.setMapOutputValueClass(Text.class);
			sortJob.setOutputKeyClass(Text.class);
			sortJob.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(sortJob, new Path(input));
			FileInputFormat.setInputDirRecursive(sortJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(output);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(sortJob, outputPath);
			
			if (sortJob.waitForCompletion(true)) {
				System.out.println(sortJob.getJobName()+" Job successed");
			} else {
				System.out.println(sortJob.getJobName()+" Job failed");
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
