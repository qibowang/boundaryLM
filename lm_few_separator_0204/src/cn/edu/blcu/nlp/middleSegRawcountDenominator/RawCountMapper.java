package cn.edu.blcu.nlp.middleSegRawcountDenominator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.awt.geom.CubicCurve2D;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2017/5/24.
 */
public class RawCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final char SEPARATOR = '▲';
	private final String SEPARATORSTR = "▲";

	private Text resKey = new Text();
	private final IntWritable ONE = new IntWritable(1);

	// Logger log = LoggerFactory.getLogger(RawCountMapper.class);

	private String ngram = "";
	private String line = "";
	private int lineLen = 0;

	private int startOrder = 1;
	private int endOrder = 7;
	private int index = 0;
	private List<String> needSuppList = new ArrayList<String>();
	private StringBuffer sb = new StringBuffer();
	private char cTemp;
	private int orderTemp;//记录当前的order
	private int ngramLen = 0;//当前ngram的长度
	private int indexTemp = 0;//用来记录index
	private String[] strArr;
	private int nTemp;//int类型的临时变量
	private String needSuppStr;//长度不满足要求，需要补充的字符串
	private String preLine = "";//当前行的上一行

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder = conf.getInt("startOrder", startOrder);
		endOrder = conf.getInt("endOrder", endOrder);
	}
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// value = transformText2UTF8(value, "gbk");
		try {
			line = new String(value.getBytes(), 0, value.getLength(), "gbk");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		line = processLine(line);//去除POS信息以及进行标点替换
		line=line.replace(" ", "");//去除SEG信息
		line=preLine+line;
		lineLen = line.length();
		if (lineLen < endOrder) {
			preLine=line;
		} else {
			preLine="";
			for (String strTemp : needSuppList) {
				sb.setLength(0);
				strArr = strTemp.split("\t");
				nTemp = Integer.parseInt(strArr[1]);
				needSuppStr = strArr[0];
				for (index = 0; index < lineLen; index++) {
					cTemp = line.charAt(index);
					if (cTemp != SEPARATOR) {
						sb.append(cTemp);
					}
					if ((needSuppStr.length() + sb.length()) == nTemp) {
						resKey.set(needSuppStr + sb.toString());
						context.write(resKey, ONE);
						break;
					}
				}
			}
			needSuppList.clear();
			for (orderTemp = startOrder; orderTemp <= endOrder; orderTemp++) {
				for (index = 0; index < lineLen - orderTemp; index++) {
					ngram = line.substring(index, index + orderTemp).trim().replace(SEPARATORSTR, "");
					ngramLen = ngram.length();
					sb.setLength(0);
					if (ngramLen < orderTemp) {
						for (indexTemp = index + orderTemp; indexTemp < lineLen; indexTemp++) {
							cTemp = line.charAt(indexTemp);
							if (cTemp != SEPARATOR) {
								sb.append(cTemp);
								if ((sb.length() + ngramLen) == orderTemp) {
									resKey.set(ngram + sb.toString());
									context.write(resKey, ONE);
									ngramLen = orderTemp;
									break;
								}
							}
						}
						ngramLen += sb.length();
						if (ngramLen < orderTemp) {
							needSuppList.add(ngram + sb.toString() + "\t" + orderTemp);
						}
					} else {
						resKey.set(ngram);
						context.write(resKey, ONE);
					}
				}
			}
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
