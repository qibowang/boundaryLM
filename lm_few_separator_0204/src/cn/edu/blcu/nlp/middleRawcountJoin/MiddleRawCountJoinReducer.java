package cn.edu.blcu.nlp.middleRawcountJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleRawCountJoinReducer extends Reducer<Text,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String valueStr;
	private String items[];
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String halfRawcountStr="";
		List<Text> list = new ArrayList<Text>();
		for(Text value:values){
			valueStr = value.toString();
			items= valueStr.split("\t");
			if(items.length==2){
				list.add(WritableUtils.clone(value, conf));
			}else{
				halfRawcountStr=valueStr;
			}
		}
		
		for(Text value:list){
			valueStr=value.toString();
			items=valueStr.split("\t");
			resKey.set(items[0]);
			resValue.set(items[1]+"\t"+halfRawcountStr);
			context.write(resKey, resValue);
		}
		
	}
}
