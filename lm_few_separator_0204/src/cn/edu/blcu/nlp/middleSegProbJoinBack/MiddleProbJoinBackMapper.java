package cn.edu.blcu.nlp.middleSegProbJoinBack;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleProbJoinBackMapper extends Mapper<Text,Text,Text,Text>{
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
