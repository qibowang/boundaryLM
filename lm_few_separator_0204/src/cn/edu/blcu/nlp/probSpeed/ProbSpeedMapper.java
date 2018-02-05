package cn.edu.blcu.nlp.probSpeed;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProbSpeedMapper extends Mapper<Text, LongWritable, Text, Text> {
	private Text resKey = new Text();
	private Text resValue = new Text();
	// private StringBuffer sb = new StringBuffer();
	private String prefix;// 前缀
	private String suffix;// 后缀
	private String ngram;
	private int wordsNum;
	private String lmFlag = "";
	Logger log = LoggerFactory.getLogger(ProbSpeedMapper.class);
	@Override
	protected void setup(Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		lmFlag = conf.get("lmFlag");
	}

	@Override
	protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ngram = key.toString();

		wordsNum = ngram.length();
		if (lmFlag.equalsIgnoreCase("left")) {
			if (wordsNum == 1) {
				resKey.set("unigram");
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			} else {
				prefix = ngram.substring(0, wordsNum - 1);

				resKey.set(prefix);
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			}
		} else if (lmFlag.equalsIgnoreCase("right")) {
			if (wordsNum == 1) {
				resKey.set("unigram");
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			} else {

				suffix = ngram.substring(1);
				resKey.set(suffix);
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			}
		}else{
			log.info("lmFlag is not set or invalid pls check again");
		}

	}

}
