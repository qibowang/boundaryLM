package cn.edu.blcu.nlp.probSpeed;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProbSpeedCombiner extends Reducer<Text, Text, Text, Text> {
	private String lmFlag;
	//private long rawCountSum=0l;
	private final char SEPARATOR = '▲';
	//private Text resKey = new Text();
	private Text resValue = new Text();
	
	private String rawCount;
	private String ngram;
	private String items[];
	private int wordsNum;
	
	private Logger log = LoggerFactory.getLogger(ProbSpeedCombiner.class);
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		lmFlag = conf.get("lmFlag");
		
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long rawCountSum=0l;
		String valueStr="";
		if (lmFlag.equalsIgnoreCase("right")) {
			/*log.info("===prob speed Combiner====");
			log.info("------------");
			log.info("sum--->"+rawCountSum);
			log.info("------------");*/
			for (Text value : values) {
				valueStr = value.toString();
				items = valueStr.split("\t");
				
				ngram = items[0];
				//log.info("ngram--->"+ngram);
				rawCount = items[1];
				//log.info("rawcount--->"+rawCount);
				
				rawCountSum += Long.parseLong(rawCount);
				//log.info("rawCountSum--->"+rawCountSum);
				if (ngram.charAt(0) == SEPARATOR) {
					//log.info("context-->"+ngram + "\t" + rawCount + "\t" + rawCountSum);
					resValue.set(valueStr + "\t" + rawCountSum);
					context.write(key, resValue);
					rawCountSum = 0;
				}
				
			}
			if(rawCountSum!=0){
				resValue.set(valueStr+"\t"+rawCountSum);
				context.write(key, resValue);
			}
		} else if (lmFlag.equalsIgnoreCase("left")) {
			for (Text value : values) {
				valueStr = value.toString();
				items = valueStr.split("\t");
				ngram = items[0];
				wordsNum=ngram.length();
				rawCount = items[1];
				rawCountSum += Long.parseLong(rawCount);
				if (ngram.charAt(wordsNum-1) == SEPARATOR) {
					resValue.set(ngram + "\t" + rawCount + "\t" + rawCountSum);
					context.write(key, resValue);
					rawCountSum = 0;
				}
			}
			if(rawCountSum!=0){
				resValue.set(valueStr+"\t"+rawCountSum);
				context.write(key, resValue);
			}
		}else{
			log.info("lmFlag is not set or invalid pls check again");
		}
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		lmFlag=conf.get("lmFlag");
		if (lmFlag.equalsIgnoreCase("right")) {
			/*if(rawCountSum!=0l){
				items=valueStr.split("\t");
				ngram=items[0];
				resKey.set(ngram.substring(1));
				log.info("*****"+valueStr+"\t"+rawCountSum);
				resValue.set(valueStr+"\t"+rawCountSum);
				context.write(resKey, resValue);
				
			}*/
		}else if(lmFlag.equalsIgnoreCase("left")){
			/*if(rawCountSum!=0l){
				items=valueStr.split("\t");
				ngram=items[0];
				wordsNum=ngram.length();
				resKey.set(ngram.substring(0,wordsNum-1));
				resValue.set(valueStr+"\t"+rawCountSum);
				context.write(resKey, resValue);
				
			}*/
		}else{
			log.info("lmFlag is not set or invalid pls check again");
		}
	}

}
