package cn.edu.blcu.nlp.middleProb0204Final;

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
	private int mid = 0;
	
	int firstIndex = 0;
	int lastIndex = 0;

	/*
	 * 该类处理没有分词信息的ngram 在2月2日版的语言模型中用去长度为奇数且mid位置是三角的当做分子的一部分，长度为偶数的部分作为分母
	 */
	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		ngram = key.toString();
		wordsNum = ngram.length();

		if (wordsNum % 2 == 1) {
			mid = wordsNum / 2;
			firstIndex = ngram.indexOf(SEP);
			lastIndex = ngram.lastIndexOf(SEP);
			if (firstIndex == lastIndex && firstIndex == mid) {
				resKey.set(ngram.replace(SEP, ""));
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			}
		} else {
			if(ngram.indexOf(SEP)==-1){
				resValue.set(String.valueOf(value.get()));
				context.write(key, resValue);
			}
		}
	}

}
