package cn.edu.blcu.nlp.middleSegProb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
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
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		for(Text value:values){
			joinValueStr=value.toString();
			items=joinValueStr.split("\t");
			if(items.length==2){
				list.add(WritableUtils.clone(value, conf));
			}else{
				denominator=Long.parseLong(joinValueStr);
			}
		}
		
		if(denominator!=0){
			for(Text value:list){
				joinValueStr=value.toString();
				items=joinValueStr.split("\t");
				ngram=items[0];
				numerator=Long.parseLong(items[1]);
				prob=(double)numerator/denominator;
				resKey.set(ngram);
				resValue.set(prob+"\t"+numerator);
				context.write(resKey, resValue);
			}
		}
	}
}
