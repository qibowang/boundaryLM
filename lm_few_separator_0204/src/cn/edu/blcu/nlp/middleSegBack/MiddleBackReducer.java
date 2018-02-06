package cn.edu.blcu.nlp.middleSegBack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleBackReducer extends Reducer<Text,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String[] items;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		long rawcountSum=0l;
		for(Text value:values){
			items=value.toString().split("\t");
			rawcountSum+=Long.parseLong(items[1]);
			list.add(WritableUtils.clone(value, conf));
		}
		for(Text value:list){
			items=value.toString().split("\t");
			resKey.set(items[0]);
			resValue.set(String.valueOf((double)Long.parseLong(items[1])/rawcountSum));
			context.write(resKey, resValue);
		}
	}
}
