package cn.edu.blcu.nlp.rawcountCrossLine;

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



public class RawcountCrossLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text resKey = new Text();
	private final IntWritable ONE = new IntWritable(1);
	private String ngram ="";
	private int startOrder=1;
	private int endOrdert=3;
	private String line;
	private int lineLen;
	private int index;
	private int orderTemp;
	private String items[];
	private String needSuppStr;
	private String preLine="";

	private List<String> list;
	Logger log = LoggerFactory.getLogger(RawcountCrossLineMapper.class);
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder = conf.getInt("startOrder", startOrder);
		endOrdert = conf.getInt("endOrder", endOrdert);
		list = new ArrayList<String>();
	}
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		line = value.toString();
		line=processLine(line);
		lineLen=line.length();
		line=preLine+line;
		line = line.replaceAll("▲+", "▲");
		line = line.replaceAll("■+", "■");
		lineLen=line.length();
		
		log.info("line--->"+line);
		if(lineLen<endOrdert){
			preLine=line;
			
		}else{
			for(String str:list){
				items=str.split("\t");
				needSuppStr=items[0];
				orderTemp=Integer.parseInt(items[1]);
				log.info("before supp--->"+str);
				for(index=0;index<orderTemp-1;index++){
					
					ngram=needSuppStr.substring(index)+line.substring(0,index+1);
					log.info("after supp--->"+ngram);
					resKey.set(ngram);
					context.write(resKey, ONE);
				}
				
			}
			list.clear();
			for(orderTemp=startOrder;orderTemp<=endOrdert;orderTemp++){
				for(index=0;index<=lineLen-orderTemp;index++){
					ngram = line.substring(index, index+orderTemp);
					resKey.set(ngram);
					context.write(resKey, ONE);
				}
				
				if(orderTemp>1){
					list.add(line.substring(index)+"\t"+orderTemp);
				}
			}
			preLine="";
			
		}
		
	}
	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		list.clear();
	}
	private String processLine(String line) {
		String posPattern = "( )?/[a-zA-Z]{1,5}( )?";
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
