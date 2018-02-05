package cn.edu.blcu.nlp.middleProb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleProbMapperDenominator extends Mapper<Text,LongWritable,Text,Text>{
	
	
	private Text resValue = new Text();
	
	@Override
	protected void map(Text key, LongWritable value, Context context)
			throws IOException, InterruptedException {
		resValue.set(String.valueOf(value.get()));
		context.write(key, resValue);
	}
	
	
}
