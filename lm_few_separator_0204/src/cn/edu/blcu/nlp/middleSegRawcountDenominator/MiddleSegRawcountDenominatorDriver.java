package cn.edu.blcu.nlp.middleSegRawcountDenominator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import com.hadoop.compression.lzo.LzoCodec;





public class MiddleSegRawcountDenominatorDriver {
	public static void main(String[] args) {
		//int order = 3;
		int tasks = 1;// 设置为7
		String input = null;
		String rawCountPath = null;
		int isLzo = 0;// 等于0表示压缩
		String corpusCodeFormat="gbk";
		int startOrder=1;
		int endOrder=3;
		boolean parameterValid=false;
		int parameterNum = args.length;
		for (int i = 0; i < parameterNum; i++) {
			//System.out.println(args[i]);
			if (args[i].equals("-input")) {
				input = args[++i];
				System.out.println("input--->" + input);
			} else if (args[i].equals("-rawcount")) {
				rawCountPath = args[++i];
				System.out.println("rawCountPath--->" + rawCountPath);
			}else if (args[i].equals("-startOrder")) {
				startOrder = Integer.parseInt(args[++i]);
				System.out.println("startOrder--->" + startOrder);
			} else if(args[i].equals("-endOrder")){
				endOrder=Integer.parseInt(args[++i]);
				System.out.println("endOrder--->" + endOrder);
			}else if (args[i].equals("-tasks")) {
				tasks = Integer.parseInt(args[++i]);
				System.out.println("tasks--->" + tasks);
			} else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			} else if(args[i].equals("-codeFormat")){
				corpusCodeFormat=args[++i].toLowerCase();
				System.out.println("corpusCodeFormat----->"+corpusCodeFormat);
			}else {
				System.out.println("there exists invalid parameters--->" + args[i]);
				parameterValid=true;
			}
			
		}
		
		if(parameterValid){
			System.out.println("parameters invalid!!!!");
			System.exit(1);
		}
		try {
			
			Configuration conf = new Configuration();
			conf.setInt("startOrder",startOrder);
			conf.setInt("endOrder", endOrder);
			//conf.set("corpusCodeFormat", corpusCodeFormat);
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			
			Job rawCountJob = Job.getInstance(conf, "middleSegRawcountDenominator job");
			System.out.println(rawCountJob.getJobName() + " is running!!!");
			rawCountJob.setJarByClass(MiddleSegRawcountDenominatorDriver.class);

			rawCountJob.setMapperClass(MiddleSegRawcountDenominatorMapper.class);
			rawCountJob.setReducerClass(MiddleSegRawcountDenominatorReducer.class);
			rawCountJob.setCombinerClass(MiddleSegRawcountDenominatorCombiner.class);
			rawCountJob.setPartitionerClass(MiddleSegRawcountDenominatorPartitioner.class);
			rawCountJob.setNumReduceTasks(tasks);

			rawCountJob.setMapOutputKeyClass(Text.class);
			rawCountJob.setMapOutputValueClass(IntWritable.class);
			rawCountJob.setOutputKeyClass(Text.class);
			rawCountJob.setOutputValueClass(LongWritable.class);

			FileInputFormat.addInputPath(rawCountJob, new Path(input));
			FileInputFormat.setInputDirRecursive(rawCountJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(rawCountPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(rawCountJob, outputPath);
			rawCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(rawCountJob);
			}

			if (rawCountJob.waitForCompletion(true)) {
				System.out.println(rawCountJob.getJobName()+" successed");
			} else {
				System.out.println(rawCountJob.getJobName()+" failed");
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