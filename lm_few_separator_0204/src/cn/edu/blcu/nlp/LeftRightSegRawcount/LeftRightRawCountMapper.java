package cn.edu.blcu.nlp.LeftRightSegRawcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeftRightRawCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	private final IntWritable ONE = new IntWritable(1);
	private Text resKey = new Text();
	private int startOrder=1;
	private int endOrder=3;
	private String line="";
	private String ngram="";
	private List<String> needSuppList = new ArrayList<String>();
	private int lineLine=0;
	private String preLine="";
	private int preLineLen=0;
	private int orderTemp=1;
	private int index=1;
	private String items[];
	private String needSuppStr="";
	
	private Logger log = LoggerFactory.getLogger(LeftRightRawCountMapper.class);
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder=conf.getInt("startOrder", startOrder);
		endOrder=conf.getInt("endOrder", endOrder);
		
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		line = value.toString();
		line=processLine(line);
		lineLine=line.length();
		log.info("preLine--->"+preLine);
		log.info("currentLine--->"+line);
		if(lineLine<2*endOrder){
			
			preLine=preLine+line;
			preLineLen=preLineLen+lineLine;
			
		}else{
			
			for(String str:needSuppList){
				items=str.split("\t");
				needSuppStr=items[0];
				log.info("needSuppStr--->"+needSuppStr);
				orderTemp=Integer.parseInt(items[1]);
				for(index=1;index<needSuppStr.length();index++){
					ngram=needSuppStr.substring(index)+line.substring(0,index);
					log.info("after supp--->"+ngram);
					resKey.set(ngram);
					context.write(resKey, ONE);
				}
				
			}
			
			for(orderTemp=startOrder;orderTemp<=endOrder;orderTemp++){
				for(index=0;index<=lineLine-orderTemp;index++){
					ngram=line.substring(index, index+orderTemp);
					log.info("ngram---->"+ngram);
					resKey.set(ngram);
					context.write(resKey, ONE);
				}
				ngram=line.substring(index+1);
				needSuppList.add(ngram+"\t"+orderTemp);
			}
			
			preLine=line;
			
		}
		
	}
	
	private String processLine(String line) {
		String posPattern = "/[a-zA-Z]{1,5}";
		String numberRegrex = "\\d+[.,]?\\d*";
		String numSign = "■";
		line = line.replaceAll(posPattern, "");
		line = line.replaceAll(numberRegrex, numSign);
		line = noneHZRep(line);
		line = line.replaceAll("(▲( ▲)*)+", "▲");
		line = line.replaceAll("(■( ■)*)+", "■");
		return line;
	}

	private static String noneHZRep(String line) {
		StringBuffer sb = new StringBuffer();
		char numSign = '■';
		char triangleSign = '▲';
		// numSign
		char[] cArr = line.toCharArray();
		for (char ch : cArr) {
			// if('\u4e00' <= ch <= '\u9fff')
			if (ch >= '\u4e00' && ch <= '\u9fff')
				sb.append(ch);
			else if (ch == numSign)
				sb.append(ch);
			else if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))
				sb.append(ch);
			else if (ch == ' ')
				sb.append(' ');
			else
				sb.append(triangleSign);
		}
		return sb.toString();
	}

}
