package cn.edu.blcu.nlp.middleLM;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleSegLMMapperProb extends Mapper<Text,Text,Text,Text>{
	String valueStr="";
	String prob="";
	String ngram="";
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		valueStr=value.toString();
		prob=valueStr.split("\t")[0];
		if(!prob.equals("1.0")){
			context.write(key, value);
		}
	}
}
