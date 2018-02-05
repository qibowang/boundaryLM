package cn.edu.blcu.nlp.mleright;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class MLEReducer extends Reducer<Text, Text, Text, Text> {
	private String flag;
	
	private String items[];
	private String ngram;
	private long rawCount;
	
	// private int order;
	private int wordsNum;
	
	private final char SEPARATOR = '▲';
	
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String valueStr="";
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		flag = conf.get("flag");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		long rawCountSum=0l;
		for(Text value:values){
			valueStr=value.toString();
			items=valueStr.split("\t");
			ngram = items[0];
			wordsNum=ngram.length();
			rawCountSum+=Long.parseLong(items[1]);
			if(ngram.charAt(wordsNum-1)==SEPARATOR){
				list.add(WritableUtils.clone(value, conf));
			}
		}
		for(Text value:list){
			valueStr=value.toString();
			items=valueStr.split("\t");
			ngram = items[0];
			rawCount=Long.parseLong(items[1]);
			resKey.set(ngram);
			if(flag.equalsIgnoreCase("prob")){
				resValue.set((double)rawCount/rawCountSum+"\t"+rawCount);
			}else if(flag.equalsIgnoreCase("back")){
				resValue.set(String.valueOf((double)rawCount/rawCountSum));
			}
			context.write(resKey, resValue);
		}
	}

}
