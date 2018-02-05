package cn.edu.blcu.nlp.back;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackMapper extends Mapper<Text, LongWritable, Text, Text> {
	private final char SEPARATOR = '▲';
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String prefix;//前缀
	private String suffix;//后缀

	private String ngram;
	private int wordsNum;
	private int order = 3;
	private String lmFlag = "";

	private Logger log = LoggerFactory.getLogger(BackMapper.class);
	@Override
	protected void setup(Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		order = conf.getInt("order", order);
		lmFlag = conf.get("lmFlag");
	}

	@Override
	protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ngram = key.toString();
		wordsNum = ngram.length();
		if (lmFlag.equalsIgnoreCase("right")) {
			if (wordsNum == 1) {
				resKey.set("unigram");
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			} else if (ngram.charAt(0) == SEPARATOR && wordsNum < order) {
				prefix = ngram.substring(0, wordsNum - 1);
				resKey.set(prefix);
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			}
		} else if (lmFlag.equalsIgnoreCase("left")) {
			if (wordsNum == 1) {
				resKey.set("unigram");
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			} else if (ngram.charAt(wordsNum-1) == SEPARATOR && wordsNum < order) {
				suffix= ngram.substring(1);
				resKey.set(suffix);
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			}
		}else{
			log.info("lmFlag is not set or invalid pls check again");
			
		}
	}

}
