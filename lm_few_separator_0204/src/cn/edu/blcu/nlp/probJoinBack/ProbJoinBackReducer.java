package cn.edu.blcu.nlp.probJoinBack;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProbJoinBackReducer extends Reducer<Text,Text,Text,Text>{
	private String valueStr;
	private String items[];
	private Text resValue = new Text();
	private Text resKey = new Text();
	private String ngram="";
	private String lmFlag="";
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		lmFlag = context.getConfiguration().get("lmFlag");
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String prob="";
		String back="0.0";
		ngram = key.toString();
		for(Text value:values){
			valueStr=value.toString().trim();
			items=valueStr.split("\t");
			if(items.length==2){
				prob=valueStr;
			}else{
				back=valueStr;
			}
		}
		resValue.set(prob+"\t"+back);
		
		if(lmFlag.equalsIgnoreCase("left")){
			ngram = new StringBuffer(ngram).reverse().toString();
			resKey.set(ngram);
			context.write(resKey, resValue);
		}else{
			context.write(key, resValue);
		}
		
	}
}
