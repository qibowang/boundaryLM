package cn.edu.blcu.nlp.middleMle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;


public class MiddleMleReducer extends Reducer<Text,Text,Text,Text>{
	private String keyStr;
	private String items[];
	private Text resKey = new Text();
	private Text resValue = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		keyStr=key.toString();
		items=keyStr.split("\t");
		long rawCountSum=0l;
		long rawCount=0l;
		if(items.length==2){
			//back
			Configuration conf = context.getConfiguration();
			List<Text> list = new ArrayList<Text>();
			
			for(Text value:values){
				items=value.toString().split("\t");
				rawCountSum+=Long.parseLong(items[1]);
				list.add(WritableUtils.clone(value, conf));
			}
			for(Text value:list){
				items=value.toString().split("\t");
				
				resKey.set(items[0]);
				resValue.set(String.valueOf(Math.log10((double)Long.parseLong(items[1])/rawCountSum)));
				context.write(resKey, resValue);
			}
		}else{
			for(Text value:values){
				items=value.toString().split("\t");
				rawCountSum=Long.parseLong(items[1]);
				rawCount=Long.parseLong(items[0]);
				resValue.set(Math.log10(2*(double)rawCount/rawCountSum)+"\t"+rawCount);
				context.write(key, resValue);
			}
		}
	}
}
