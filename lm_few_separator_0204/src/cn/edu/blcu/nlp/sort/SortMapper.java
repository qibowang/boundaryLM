package cn.edu.blcu.nlp.sort;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text,Text,Text,Text>{
	
	
	private String valueStr;
	private String items[];
	double prob=0.0;
	double back=0.0;
	private Text resValue = new Text();
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		valueStr=value.toString();
		items=valueStr.split("\t");
		prob=Math.log10(Double.parseDouble(items[0]));
		back=Math.log10(Double.parseDouble(items[2]));
		resValue.set(prob+"\t"+items[1]+"\t"+back);
		context.write(key, resValue);
	}
}
