package cn.edu.blcu.nlp.mleright;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;


//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
 * 
 * MLE利用最大似然估计对概率进行计算
 * 主要分类三类 取前缀和后缀作为left和right部分 进行模型计算
 * 中间部分的模型对中间为三角的部分进行处理公式为：序列count/(左半部分count+右半部分count)
 * 以  [词序列+"\t"+fileFlag]作为 resKey
 * 
 * 
 * */

public class MLEMapper extends Mapper<Text, LongWritable, Text, Text> {
	
	//private final char SEPARATOR = '▲';
	private Text outKey = new Text();
	private Text outValue = new Text();
	private String flag;
	private String unigramFlag="unigram";
	private int wordsNum = 0;
	private long rawCount = 0l;
	private String ngram = "";
	private String prefix = "";
	private String suffix = "";
	
	// Logger log =LoggerFactory.getLogger(MLEMapper.class);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		flag = conf.get("flag");

	}

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {

		ngram = key.toString();
		ngram = new StringBuffer(ngram).reverse().toString();
		wordsNum = ngram.length();
		rawCount = value.get();
		outValue.set(ngram + "\t" + rawCount);
		if(flag.equalsIgnoreCase("prob")){
			
			if(1 == wordsNum){
				outKey.set(unigramFlag);
				context.write(outKey, outValue);
			}else{
				prefix = ngram.substring(0, wordsNum - 1);
				outKey.set(prefix);
				context.write(outKey, outValue);
			}
		}else if(flag.equalsIgnoreCase("back")){
			
			if(1 == wordsNum){
				outKey.set(unigramFlag);
				context.write(outKey, outValue);
			}else{
				suffix = ngram.substring(1);
				outKey.set(suffix);
				context.write(outKey, outValue);
			}
		}
	}

}
