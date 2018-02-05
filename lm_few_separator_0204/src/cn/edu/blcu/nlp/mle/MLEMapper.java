package cn.edu.blcu.nlp.mle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MLEMapper extends Mapper<Text,LongWritable,Text,Text>{
	private int order=3;
	private String lmFlag=null;
	private long rawcount = 0l;
	private String suffix;//后缀
	private String prefix;//前缀
	private final String probFlag="PROB";
	private final String backFlag="BACK";
	private String ngram;
	private int wordsNum;
	private Text resKey = new Text();
	private Text resValue = new Text();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		order = conf.getInt("order", order);
		lmFlag = conf.get("lmFlag");
	}
	@Override
	protected void map(Text key, LongWritable value,Context context)
			throws IOException, InterruptedException {
		ngram = key.toString();
		if(lmFlag.equals("right")){
			ngram = new StringBuffer(ngram).reverse().toString();
		}
		rawcount = value.get();
		wordsNum = ngram.length();
		prefix = ngram.substring(0, wordsNum-1);
		suffix = ngram.substring(1);
		
		if(wordsNum==1){
			resKey.set(probFlag+"\t"+"unigram");
			resValue.set(ngram+"\t"+rawcount);
			context.write(resKey, resValue);
			
			resKey.set(backFlag+"\t"+"unigram");
			resValue.set(ngram+"\t"+rawcount);
			context.write(resKey, resValue);
		}else if(wordsNum>1&&wordsNum<order){
			resKey.set(probFlag+"\t"+prefix);
			resValue.set(ngram+"\t"+value.get());
			context.write(resKey, resValue);
			
			resKey.set(backFlag+"\t"+suffix);
			resValue.set(ngram+"\t"+rawcount);
			context.write(resKey, resValue);
		}else if(wordsNum==order){
			resKey.set(probFlag+"\t"+prefix);
			resValue.set(ngram+"\t"+rawcount);
			context.write(resKey, resValue);
		}
	}
}
