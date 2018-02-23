package cn.edu.blcu.nlp.middleSegRawcountNumerator0220;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class NumeratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable ONE = new IntWritable(1);
	private Text resKey = new Text();
	private int startOrder;//
	private int endOrder;
	private String currentLine = "";// 要处理的当前行

	private int senLen=0;
	private final char SEP_CHAR = '▲';// 分隔符字符
	private final String SEP_STRING = "▲";// 分隔符字符串
	private final char SEG_CHAR = ' ';
	private final String SEG_STRING = " ";
	// private final String SEG_STRING=" ";
	private String ngram = "";// ngram串
	private int currentOrder = 0;
	private StringBuffer leftSb = new StringBuffer();// 分隔符左侧的StringBuffer对象
	private StringBuffer rightSb = new StringBuffer();// 分隔符右侧的StringBuffer对象
	
	private String sLeft;// 分隔符左侧的字符串
	private String sRight;// 分隔符右侧的字符串
	private int sLeftLen=0;// 分隔符左侧的字符串的长度
	private int sRightLen=0;//// 分隔符右侧的字符串的长度
	private char cTemp;
	private List<Integer> blankIndexList = new ArrayList<Integer>();// 各个空格位置索引的list
	
	private String[] senArrTemp;
	

	private String corpusCodeFormat = "gbk";
	private int tempIndex = 0;

	private int leftIndex = 0;
	private int rightIndex = 0;
	private boolean leftSEGSatisfy = false;
	//private boolean leftLenSatisfy = false;
	private boolean rightSEGSatisfy = false;
	//private boolean rightLenSatisfy = false;


	//Logger log = LoggerFactory.getLogger(NumeratorMapper.class);

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// startOrder和endOrder都必须是奇数
		startOrder = conf.getInt("startOrder", 0);
		endOrder = conf.getInt("endOrder", 3);
		corpusCodeFormat = conf.get("corpusCodeFormat", corpusCodeFormat);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			currentLine = new String(value.getBytes(), 0, value.getLength(), "gbk");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		currentLine = processLine(currentLine);
		//log.info("line--->"+currentLine);
		senArrTemp=currentLine.split(SEP_STRING);
		for(String sen:senArrTemp){
			sen=sen.trim();
			senLen=sen.length();
			if(senLen<startOrder)
				continue;
			if(sen.replaceAll(SEG_STRING, "").length()<startOrder)
				continue;
			blankIndexList = sepCount(sen, senLen,SEG_CHAR);
		//	log.info("sen===>"+sen);
			for(currentOrder=startOrder;currentOrder<=endOrder;currentOrder++){
				for(int blankIndex:blankIndexList){
					for(tempIndex=1;tempIndex<currentOrder;tempIndex++){
						leftSEGSatisfy=false;
						rightSEGSatisfy=false;
						sLeftLen=0;
						sRightLen=0;
						leftSb.setLength(0);
						rightSb.setLength(0);
						for (leftIndex = blankIndex - 1; leftIndex >= 0; leftIndex--) {
							cTemp = sen.charAt(leftIndex);
							
							if (cTemp != SEG_CHAR) {
								leftSb.append(cTemp);
								sLeftLen++;
							}
							if (sLeftLen == tempIndex) {
								
								if((leftIndex==0)||(leftIndex>0&&sen.charAt(leftIndex-1)==SEG_CHAR))
									leftSEGSatisfy=true;
								break;
							}
						}
						if(!leftSEGSatisfy)
							continue;
						sLeft=leftSb.reverse().toString();
						//log.info("sleft--->"+sLeft);
						for (rightIndex = blankIndex + 1; rightIndex < senLen; rightIndex++) {
							cTemp = sen.charAt(rightIndex);
							
							if (cTemp != SEG_CHAR) {
								rightSb.append(cTemp);
								sRightLen++;
							}
							if (sRightLen == currentOrder -tempIndex) {
								
								if((rightIndex==senLen-1)||(rightIndex<senLen-1&&sen.charAt(rightIndex+1)==SEG_CHAR))
									rightSEGSatisfy=true;
								break;
							}
						}
						if(!rightSEGSatisfy)
							continue;
						sRight=rightSb.toString();
						ngram = sLeft +SEP_CHAR+ sRight;
					//	log.info("ngram-->"+ngram);
						resKey.set(ngram);
						context.write(resKey, ONE);
					}
				}
			}
		}
	}

	private List<Integer> sepCount(String line, int lineLen, char c) {
		List<Integer> indexList = new ArrayList<Integer>();
		for (int i = 0; i < lineLen; i++) {
			if (line.charAt(i) == c)
				indexList.add(i);
		}
		return indexList;
	}

	private String processLine(String line) {
		String posPattern = "/[a-zA-Z]{1,5}";
		String numberRegrex = "\\d+[.,]?\\d*";
		String numSign = "■";
		// String sepSign = "▲";
		line = line.replaceAll(posPattern, "");
		line = line.replaceAll(numberRegrex, numSign);
		// line = line.replaceAll(" ", sepSign);
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
