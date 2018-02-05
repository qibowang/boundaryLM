package cn.edu.blcu.nlp.middleBack0202;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleBackReducer extends Reducer<Text, Text, Text, Text> {
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String[] items;
	private int wordsNum;
	private int mid;
	private String ngram;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		long rawcountSum = 0l;
		for (Text value : values) {
			items = value.toString().split("\t");
			rawcountSum += Long.parseLong(items[0]);
			list.add(WritableUtils.clone(value, conf));
		}
		/*for (Text value : list) {
			items = value.toString().split("\t");
			if (items.length == 2) {
				resKey.set(items[1]);
				resValue.set(String.valueOf((double) Long.parseLong(items[0]) / rawcountSum));
				context.write(resKey, resValue);
			}
		}*/
		for (Text value : list) {
			items = value.toString().split("\t");
			ngram = items[1];
			wordsNum=ngram.length();
			
			if (items.length == 2) {
				resKey.set(items[1]);
				resValue.set(String.valueOf((double) Long.parseLong(items[0]) / rawcountSum));
				context.write(resKey, resValue);
			}
		}
	}
}
