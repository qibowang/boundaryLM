package cn.edu.blcu.nlp.middleLM;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MiddleSegLMMapperBack extends Mapper<Text,Text,Text,Text>{
	String ngram;
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		ngram=key.toString();
		if(ngram.length()<7){
			context.write(key, value);
		}
	}
}
