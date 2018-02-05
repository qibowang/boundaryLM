package cn.edu.blcu.nlp.mle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class MLEReducer extends Reducer<Text,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	private final char SEPARATOR = '▲';
	private final String probFlag="PROB";
	private final String backFlag="BACK";
	
	private String ngram;
	private String valueStr;
	private String items[];
	private int wordsNum = 0;
	private String rawCountStr;
	//Logger log = LoggerFactory.getLogger(MLEReducer.class);
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		//log.info("====start====");
		//log.info("---->"+key.toString());
		
		long rawCountSum = 0l;
		String flag= key.toString().split("\t")[0];
		//log.info("flag---->"+flag);
		List<Text> list = new ArrayList<Text>();
		for(Text value:values){
			valueStr = value.toString();
			//log.info("====="+valueStr);
			items= valueStr.split("\t");
			ngram=items[0];
			wordsNum = ngram.length();
			rawCountStr=items[1];
			rawCountSum+=Long.parseLong(rawCountStr);
			if(ngram.charAt(wordsNum-1)==SEPARATOR){
				//log.info("*****"+valueStr);
				list.add(WritableUtils.clone(value, conf));
			}
		}
		for(Text value:list){
			valueStr = value.toString();
			items=valueStr.split("\t");
			ngram=items[0];
			ngram = new StringBuffer(ngram).reverse().toString();
			rawCountStr=items[1];
			resKey.set(ngram);
			if(flag.equals(probFlag)){
				resKey.set(ngram);
				//resValue.set(rawCountStr+"/"+rawCountSum+"\t"+rawCountStr);
				resValue.set(Math.log10((double)Long.parseLong(rawCountStr)/rawCountSum)+"\t"+rawCountStr);
				context.write(resKey, resValue);
			}else if(flag.equals(backFlag)){
				resKey.set(ngram);
				//resValue.set(rawCountStr+"/"+rawCountSum);
				resValue.set(String.valueOf(Math.log10((double)Long.parseLong(rawCountStr)/rawCountSum)));
				context.write(resKey, resValue);
			}
		}
		//log.info("===end===");
		
	}
}
