package cn.edu.blcu.nlp.middleMle;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class MiddleMleMapper extends Mapper<Text, Text, Text, Text> {
	
	private int wordsNum;
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String ngram;
	private final String backFlag ="BACK";
	private int order=3;
	//Logger log = LoggerFactory.getLogger(MiddleMleMapper.class);
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		order = context.getConfiguration().getInt("order", order);
	}
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		//key:ngram
		//value:rawcount sum(leftcount+rightcount)
		//log.info("==========");
		context.write(key, value);
		ngram = key.toString();
		wordsNum=ngram.length();
		//log.info("ngram--->"+ngram);
		//back
		if(wordsNum==1){
			resKey.set(backFlag+"\t"+ngram);
			resValue.set(ngram+"\t"+value.toString().split("\t")[0]);
			context.write(resKey, resValue);
		}else if(wordsNum >1 && wordsNum < order){
			resKey.set(backFlag+"\t"+ngram.substring(1,wordsNum-1));
			//log.info("back--->"+ngram.substring(1,wordsNum-1));
			resValue.set(ngram+"\t"+value.toString().split("\t")[0]);
			context.write(resKey, resValue);
		}
	}
}
