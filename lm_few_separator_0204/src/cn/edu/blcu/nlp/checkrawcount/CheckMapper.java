package cn.edu.blcu.nlp.checkrawcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckMapper extends Mapper<Text,LongWritable,Text,IntWritable>{
	private Text resKey = new Text();
	private IntWritable ONE = new IntWritable();
	private final char SEP='▲';
	private String ngram;
	private int wordsNum;
	private int sepNum;
	Logger log = LoggerFactory.getLogger(CheckMapper.class);
	@Override
	protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		ngram = key.toString();
		
		wordsNum= ngram.length();
		sepNum=getSepNum(ngram, wordsNum);
		log.info("ngram--->"+ngram);
		log.info("==>"+wordsNum+"\t"+sepNum);
		resKey.set(wordsNum+"\t"+sepNum);
		context.write(resKey, ONE);
	}
	private int getSepNum(String ngram,int wordsNum){
		int num=0;
		for(int i=0;i<wordsNum;i++){
			if(ngram.charAt(i)==SEP){
				num++;
			}
		}
		return num;
	}
}
