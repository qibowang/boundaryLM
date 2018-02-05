package cn.edu.blcu.nlp.middleBack0202;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleBackSEGMapper extends Mapper<Text, LongWritable, Text, Text> {
	
	private String ngram = "";
	private int wordsNum = 0;
	private Text resKey = new Text();
	private Text resValue = new Text();

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		ngram = key.toString();
		wordsNum = ngram.length();

		resKey.set(ngram.substring(1, wordsNum - 1));
		resValue.set(value.get()+"\t"+ngram);
		context.write(resKey, resValue);

	}

}
