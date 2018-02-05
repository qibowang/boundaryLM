package cn.edu.blcu.nlp.back;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackReducer extends Reducer<Text, Text, Text, Text> {

	private final char SEPARATOR = '▲';
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String valueStr = "";
	private String ngram = "";
	private long rawcount = 0l;
	private String items[];
	private String lmFlag;
	private int wordsNum;
	private int gtmin=0;
	Logger log = LoggerFactory.getLogger(BackReducer.class);
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		lmFlag = conf.get("lmFlag");
		gtmin=conf.getInt("gtmin", gtmin);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long rawCountSum = 0l;
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		if (lmFlag.equalsIgnoreCase("right")) {
			for (Text value : values) {
				valueStr = value.toString();
				items = valueStr.split("\t");
				ngram = items[0];
				rawcount = Long.parseLong(items[1]);
				rawCountSum += rawcount;
				if (ngram.charAt(0) == SEPARATOR) {
					list.add(WritableUtils.clone(value, conf));
				}
			}
		} else if (lmFlag.equalsIgnoreCase("left")) {
			for (Text value : values) {
				valueStr = value.toString();
				items = valueStr.split("\t");
				ngram = items[0];
				wordsNum=ngram.length();
				rawcount = Long.parseLong(items[1]);
				rawCountSum += rawcount;
				if (ngram.charAt(wordsNum-1) == SEPARATOR) {
					list.add(WritableUtils.clone(value, conf));
				}
			}
		}else{
			log.info("lmFlag is not set or invalid pls check again");
		}

		for (Text value : list) {
			valueStr = value.toString();
			items = valueStr.split("\t");
			ngram = items[0];
			rawcount = Long.parseLong(items[1]);
			if(rawcount>=gtmin){
				resKey.set(ngram);
				resValue.set(String.valueOf(Math.log10((double) rawcount / rawCountSum)));
				context.write(resKey, resValue);
			}
		}
	}
}
