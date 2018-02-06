package cn.edu.blcu.nlp.middleSegBack;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleBackJoinReducer extends Reducer<Text, Text, Text, Text> {
	private Text resValue = new Text();
	@Override
	protected void reduce(Text ngram, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double backSum=0.0;
		int count =0;
		for(Text value:values){
			backSum+=Double.parseDouble(value.toString());
			count++;
		}
		if(count==2){
			resValue.set(String.valueOf(backSum/2));
			context.write(ngram, resValue);
		}else{
			resValue.set(String.valueOf(backSum));
			context.write(ngram, resValue);
		}
		
	}
}
