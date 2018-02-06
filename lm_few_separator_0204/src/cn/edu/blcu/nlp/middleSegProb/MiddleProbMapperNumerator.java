package cn.edu.blcu.nlp.middleSegProb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleProbMapperNumerator extends Mapper<Text, LongWritable, Text, Text> {
	private final String SEP = "▲";
	private String ngram = "";
	private int wordsNum = 0;
	private Text resKey = new Text();
	private Text resValue = new Text();

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		ngram = key.toString();
		wordsNum = ngram.length();
		if (wordsNum > 1) {
			resKey.set(ngram.replace(SEP, ""));
			resValue.set(ngram + "\t" + value.get());
			context.write(resKey, resValue);
		}
	}

}
