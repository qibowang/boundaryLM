package cn.edu.blcu.nlp.middleProb0204Final;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleProbMapperDenominator extends Mapper<Text,LongWritable,Text,Text>{
	
	/*
	 * 该类处理基于分词信息的ngram
	 * 在2月2日版的语言模型中用来做分子的一部分
	 * */
	private Text resValue = new Text();
	
	@Override
	protected void map(Text key, LongWritable value, Context context)
			throws IOException, InterruptedException {
		resValue.set(key.toString()+"\t"+value.get());
		context.write(key, resValue);
	}
	
	
}
