package cn.edu.blcu.nlp.middleProbJoinBack;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleProbJoinBackReducer extends Reducer<Text,Text,Text,Text>{
	
	private String[] items;
	private String valueStr;

	private Text resValue = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String prob="";
		String back="0.0";
		for(Text value:values){
			valueStr=value.toString().trim();
			items=valueStr.split("\t");
			if(items.length==2){
				prob=valueStr;
			}else{
				back=valueStr;
			}
		}
		
		if(prob.length()!=0){
			resValue.set(prob+"\t"+back);
			context.write(key, resValue);
		}
	}
	
}
