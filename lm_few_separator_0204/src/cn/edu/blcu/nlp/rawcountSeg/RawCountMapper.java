package cn.edu.blcu.nlp.rawcountSeg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2017/5/24.
 */
public class RawCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// private final char SEPARATOR='▲';
	private int order;
	private Text resKey = new Text();
	private final IntWritable ONE = new IntWritable(1);

	// Logger log = LoggerFactory.getLogger(RawCountMapper.class);
	
	private String ngram = "";
	private String line = "";
	private String tempLine = "";
	private List<String> tempList = new ArrayList<String>();// 下标比ngram的n小2

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		order = conf.getInt("order", 3);
		for (int i = 0; i <= order - 2; i++) {
			tempList.add("");
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// value = transformText2UTF8(value, "gbk");
		line = value.toString().trim();
		for (int i = 1; i <= order; i++) {
			tempLine = (i == 1) ? line : tempList.get(i - 2) + line;
			tempLine = tempLine.trim();
			int length = tempLine.length();
			for (int j = 0; j <= length - i; j++) {
				ngram = tempLine.substring(j, j + i).trim();
				resKey.set(ngram);
				context.write(resKey, ONE);
			}
			if (i > 1) {
				tempList.set(i - 2, tempLine.substring(length - i + 1, length));
			}
		}

	}
}
