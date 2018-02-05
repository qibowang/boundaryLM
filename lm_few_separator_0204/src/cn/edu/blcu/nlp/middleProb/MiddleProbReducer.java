package cn.edu.blcu.nlp.middleProb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleProbReducer extends Reducer<Text,Text,Text,Text>{
	
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String[] items;
	private String joinValueStr="";
	private String ngram;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long numerator=0l;
		long denominator=0l;
		double prob=0.0;
		for(Text value:values){
			joinValueStr=value.toString();
			items=joinValueStr.split("\t");
			if(items.length==2){
				ngram = items[0];
				numerator=Long.parseLong(items[1]);
			}else{
				denominator=Long.parseLong(joinValueStr);
			}
		}
		
		if(numerator!=0&&denominator!=0){
			resKey.set(ngram);
			prob=(double)numerator/(numerator+denominator);
			resValue.set(String.valueOf(prob)+"\t"+numerator);
			context.write(resKey, resValue);
		}

	}
}
