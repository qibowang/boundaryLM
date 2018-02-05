package cn.edu.blcu.nlp.middleRawcountJoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiddleLeftJoinRightReducer extends Reducer<Text,Text,Text,Text>{
	private Text resValue = new Text();
	private String valueStr;
	private String items[];
	boolean isValid=false;
	Logger log = LoggerFactory.getLogger(MiddleLeftJoinRightReducer.class);
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		log.info("----------");
		long rawCount = 0l;
		long rawCountSum = 0l;
		log.info("key--->"+key.toString());
		for(Text value:values){
			valueStr=value.toString().trim();
			log.info("value--->"+valueStr);
			items=valueStr.split("\t");
			if(items.length==2){
				isValid=true;
				rawCount=Long.parseLong(items[0]);
				rawCountSum+=Long.parseLong(items[1]);
			}
		}
		if(isValid){
			resValue.set(rawCount+"\t"+rawCountSum);
			context.write(key, resValue);
		}
	}
}
