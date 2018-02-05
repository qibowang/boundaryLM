package cn.edu.blcu.nlp.middleProb0202;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleProbReducer extends Reducer<Text, Text, Text, Text> {

	private Text resKey = new Text();
	private Text resValue = new Text();
	private String[] items;
	private String joinValueStr = "";
	private String ngram;
	private int wordsNum=0;
	private int mid=0;
	private final String SEP = "▲";
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long numeratorSEP = 0l;
		long numeratorSEG = 0l;
		long denominatorSum = 0l;
		long numeratorSum = 0l;
		String sepKeyStr = "";
		String segKeyStr = "";
		
		// long numeratorSum = 0l;
		// long denominatorSum = 0l;
		double prob = 0.0;
		for (Text value : values) {
			joinValueStr = value.toString();
			items = joinValueStr.split("\t");
			if (items.length == 2) {
				// ngram wordcount
				ngram = items[0];
				if (ngram.length() % 2 == 1) {
					sepKeyStr = ngram;
					numeratorSEP = Long.parseLong(items[1]);
				} else {
					segKeyStr = ngram;
					numeratorSEG = Long.parseLong(items[1]);
				}
			} else {
				denominatorSum += Long.parseLong(joinValueStr);
			}
		}
		
		numeratorSum = numeratorSEG + numeratorSEP;
		denominatorSum += numeratorSEG;
		if (numeratorSEP != 0 && denominatorSum != 0) {
			
			if (sepKeyStr.length() != 0) {
				resKey.set(sepKeyStr);
				prob = (double) numeratorSum / denominatorSum;
				resValue.set(String.valueOf(prob) + "\t" + numeratorSum);
				context.write(resKey, resValue);
				
			} else if (sepKeyStr.length()==0&&segKeyStr.length()!=0) {
				wordsNum=segKeyStr.length();
				mid=wordsNum/2;
				resKey.set(segKeyStr.substring(0, mid)+SEP+segKeyStr.substring(mid));
				prob = (double) numeratorSum / denominatorSum;
				resValue.set(String.valueOf(prob) + "\t" + numeratorSum);
				context.write(resKey, resValue);
			}
		}

		/*
		 * if (numeratorSum != 0 && denominatorSum != 0 && keyStr.length() != 0)
		 * { resKey.set(keyStr); prob = (double) numeratorSum /
		 * (denominatorSum+numeratorSEG); resValue.set(String.valueOf(prob) +
		 * "\t" + numeratorSEP); context.write(resKey, resValue); }
		 */

	}
}
