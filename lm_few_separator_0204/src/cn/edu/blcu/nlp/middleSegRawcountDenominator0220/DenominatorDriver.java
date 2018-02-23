package cn.edu.blcu.nlp.middleSegRawcountDenominator0220;

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

public class DenominatorDriver {
	public static void main(String[] args) {
		int startOrder = 1;
		int endOrder = 3;
		int tasks = 1;// 设置为7
		String input = null;
		String rawCountPath = null;
		int isLzo = 0;// 等于0表示压缩
		String lmFlag="";
		boolean parameterValid=false;
		int parameterNum = args.length;
		
		for (int i = 0; i < parameterNum; i++) {
			if (args[i].equals("-input")) {
				input = args[++i];
				System.out.println("input--->" + input);
			} else if (args[i].equals("-output")) {
				rawCountPath = args[++i];
				System.out.println("rawCountPath--->" + rawCountPath);
			} else if (args[i].equals("-startOrder")) {
				startOrder = Integer.parseInt(args[++i]);
				System.out.println("startOrder--->" + startOrder);
			} else if (args[i].equals("-endOrder")) {
				endOrder = Integer.parseInt(args[++i]);
				System.out.println("endOrder--->" + endOrder);
			} else if (args[i].equals("-tasks")) {
				tasks = Integer.parseInt(args[++i]);
				System.out.println("tasks--->" + tasks);
			} else if (args[i].equals("-isLzo")) {
				isLzo = Integer.parseInt(args[++i]);
				System.out.println("isLzo---->" + isLzo);
			} else if(args[i].equals("-lmFlag")){
				lmFlag=args[++i];
				System.out.println("lmFlag--->"+lmFlag);
			}else {
				System.out.println("there exists invalid parameters--->" + args[i]);
				
			}

		}

		if(parameterValid){
			System.out.println("parameters invalid!!!!");
			System.exit(1);
		}
		
		try {

			Configuration conf = new Configuration();
			conf.setInt("startOrder", startOrder);
			conf.setInt("endOrder", endOrder);

			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);

			Job denominatorRawCountJob = Job.getInstance(conf, "denominator rawCountJob");
			System.out.println(denominatorRawCountJob.getJobName() + " is running!!!");
			denominatorRawCountJob.setJarByClass(DenominatorDriver.class);
			denominatorRawCountJob.setMapperClass(DenominatorMapper.class);
			denominatorRawCountJob.setReducerClass(DenominatorReducer.class);
			denominatorRawCountJob.setCombinerClass(DenominatorCombiner.class);
			denominatorRawCountJob.setPartitionerClass(DenominatorPartitioner.class);
			denominatorRawCountJob.setNumReduceTasks(tasks);

			denominatorRawCountJob.setMapOutputKeyClass(Text.class);
			denominatorRawCountJob.setMapOutputValueClass(IntWritable.class);
			denominatorRawCountJob.setOutputKeyClass(Text.class);
			denominatorRawCountJob.setOutputValueClass(LongWritable.class);

			FileInputFormat.addInputPath(denominatorRawCountJob, new Path(input));
			FileInputFormat.setInputDirRecursive(denominatorRawCountJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(rawCountPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(denominatorRawCountJob, outputPath);
			denominatorRawCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(denominatorRawCountJob);
			}

			if (denominatorRawCountJob.waitForCompletion(true)) {
				System.out.println(denominatorRawCountJob.getJobName() + " successed");
			} else {
				System.out.println(denominatorRawCountJob.getJobName() + " failed");
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
