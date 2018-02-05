package cn.edu.blcu.nlp.middleRawcountJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleRawCountJoinMapper extends Mapper<Text, LongWritable, Text, Text> {
	private String ngram;
	private int wordsNum;
	private final char SEPARATOR = '▲';
	private Text resKey = new Text();
	private Text resValue = new Text();
	private int middleIndex;
	private long rawcount;
	private int order = 3;
	private int end=0;

	@Override
	protected void setup(Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		order = context.getConfiguration().getInt("order", order);
		end=order/2+1;
	}

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		ngram = key.toString();
		wordsNum = ngram.length();
		rawcount = value.get();
		middleIndex = wordsNum / 2;
		if(wordsNum > 1 && wordsNum <= end){
			if (ngram.charAt(0) == SEPARATOR ) {
				resValue.set(String.valueOf(rawcount));
				context.write(key, resValue);
			}
			if(ngram.charAt(wordsNum - 1) == SEPARATOR){
				resValue.set(String.valueOf(rawcount));
				context.write(key, resValue);
			}
		}
		if(wordsNum>1&&wordsNum%2==1&&ngram.charAt(middleIndex) == SEPARATOR){
			
			resKey.set(ngram.substring(0, middleIndex + 1));
			resValue.set(ngram + "\t" + rawcount);
			context.write(resKey, resValue);
			
			resKey.set(ngram.substring(middleIndex));
			resValue.set(ngram + "\t" + rawcount);
			context.write(resKey, resValue);
		}
		
	}
}
