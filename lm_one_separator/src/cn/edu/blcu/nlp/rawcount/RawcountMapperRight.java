package cn.edu.blcu.nlp.rawcount;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawcountMapperRight extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text resKey = new Text();
	private final IntWritable ONE = new IntWritable(1);
	private final char SEPARATOR_CHAR = '▲';
	private final String SEPARATOR_STRING = "▲";
	private String ngram = "";
	private int startOrder = 1;
	private int endOrdert = 3;
	private String line;
	private int lineLen;
	private int index;
	private int suppIndex = 0;
	private int orderTemp;
	private String items[];
	private String needSuppStr;
	private String preLine = "";
	private int wordsNum;
	private int needSuppNum = 0;
	private List<String> list;
	private char cTmp;
	private StringBuffer sbTmp = new StringBuffer();
	Logger log = LoggerFactory.getLogger(RawcountMapperRight.class);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder = conf.getInt("startOrder", startOrder);
		endOrdert = conf.getInt("endOrder", endOrdert);
		list = new ArrayList<String>();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			line = new String(value.getBytes(), 0, value.getLength(), "gbk");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		line = processLine(line);

		line = preLine + line;
		line = line.replaceAll("▲+", "▲");
		line = line.replaceAll("■+", "■");
		lineLen = line.length();

		//log.info("line--->" + line);
		if (lineLen < endOrdert) {
			preLine = line;
		} else {
			for (String str : list) {
				items = str.split("\t");
				if (items.length == 2) {
					needSuppStr = items[0];
					orderTemp = Integer.parseInt(items[1]);
					//log.info("cross line before supp--->" + str);
					for (index = 0; index < orderTemp - 1; index++) {
						ngram = needSuppStr.substring(index) + line.substring(0, index + 1);
						ngram = removeRedundantSpparator(ngram, SEPARATOR_STRING);
						wordsNum = ngram.length();
						needSuppNum = orderTemp - wordsNum;
						if (needSuppNum == 0) {
							resKey.set(ngram);
						//	log.info("ngram--->"+ngram);
							context.write(resKey, ONE);
						} else {
							sbTmp.setLength(0);
							for (suppIndex = index + 1; suppIndex < lineLen; suppIndex++) {
								cTmp = line.charAt(suppIndex);
								if (cTmp != SEPARATOR_CHAR) {
									sbTmp.append(cTmp);
									needSuppNum--;
								}
								if (needSuppNum == 0) {
									resKey.set(ngram + sbTmp.toString());
							//		log.info("ngram====>"+resKey.toString());
									context.write(resKey, ONE);
									break;
								}
							}
						}
						//log.info("cross line after supp--->" + resKey.toString());
					}
				}else if(items.length==3){
					sbTmp.setLength(0);
					//log.info("inner cross line before supp--->" + str);
					needSuppStr = items[0];
					needSuppStr=removeRedundantSpparator(needSuppStr, SEPARATOR_STRING);
					wordsNum=needSuppStr.length();
					
					
					orderTemp = Integer.parseInt(items[1]);
					needSuppNum=orderTemp-wordsNum;
					for(suppIndex = 0; suppIndex < lineLen; suppIndex++){
						cTmp = line.charAt(suppIndex);
						if (cTmp != SEPARATOR_CHAR) {
							sbTmp.append(cTmp);
							needSuppNum--;
						}
						if (needSuppNum == 0) {
							resKey.set(needSuppStr + sbTmp.toString());
						//	log.info("inner cross line after supp--->" + resKey.toString());
							context.write(resKey, ONE);
							break;
						}
					} 
				}
			}
			list.clear();
			for (orderTemp = startOrder; orderTemp <= endOrdert; orderTemp++) {
				for (index = 0; index <= lineLen - orderTemp; index++) {
					ngram = line.substring(index, index + orderTemp);
					log.info("ngram---->"+ngram);
					if(orderTemp==1){
						resKey.set(ngram);
						context.write(resKey, ONE);
					}else{
						ngram = removeRedundantSpparator(ngram, SEPARATOR_STRING);
						wordsNum = ngram.length();
						needSuppNum = orderTemp - wordsNum;
						if (needSuppNum == 0) {
							resKey.set(ngram);
							context.write(resKey, ONE);
						} else {
							sbTmp.setLength(0);
							for (suppIndex = index + orderTemp; suppIndex < lineLen; suppIndex++) {
								cTmp = line.charAt(suppIndex);
								if (cTmp != SEPARATOR_CHAR) {
									sbTmp.append(cTmp);
									needSuppNum--;
								}
								if (needSuppNum == 0) {
									resKey.set(ngram + sbTmp.toString());
									context.write(resKey, ONE);
									break;
								}
							}						
							if(needSuppNum != 0){
								list.add(ngram + sbTmp.toString() + "\t" + orderTemp+"\t"+" ");
							}
						}
					
					}
				}
				if (orderTemp > 1) {
					list.add(line.substring(index) + "\t" + orderTemp);
				}
			}
			preLine = "";
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
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

	private String removeRedundantSpparator(String ngram, String separatorStr) {

		if (ngram.charAt(0) == SEPARATOR_CHAR) {
			ngram = ngram.replaceAll(separatorStr, "");
			return separatorStr+ngram;
			
		} else {
			ngram = ngram.replaceAll(separatorStr, "");
			return ngram;
		}

	}
}
