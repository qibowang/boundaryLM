package cn.edu.blcu.nlp.middleSegRawcountNumerator;

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

public class MiddleSegRawCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable ONE = new IntWritable(1);
	private Text resKey = new Text();
	private int startOrder;//
	private int endOrder;
	private String currentLine = "";// 要处理的当前行
	private int currentLineLen = 0;// 要处理的当前行的长度
	private String preLine = "";// 当前行的上一行
	private int preLineLen = 0;
	// private final String sepString = "▲";// 分隔符字符串
	private final char sepChar = '▲';// 分隔符字符
	private String ngram = "";// ngram串
	private int currentOrder = 0;
	private StringBuffer leftSb = new StringBuffer();// 分隔符左侧的StringBuffer对象
	private StringBuffer rightSb = new StringBuffer();// 分隔符右侧的StringBuffer对象
	private StringBuffer rightSuppSub = new StringBuffer();// 分隔符右侧的长度不满足要求是需要进行向下一行扩展，为了避免频繁的String+=,因此定义该对象
	private String sLeft;// 分隔符左侧的字符串
	private String sRight;// 分隔符右侧的字符串
	private int sLeftLen;// 分隔符左侧的字符串的长度
	private int sRightLen;//// 分隔符右侧的字符串的长度
	char cTemp;
	private List<String> currentLineList = new ArrayList<String>();// 当前行需要补充的list
	private List<Integer> blankIndexList = new ArrayList<Integer>();// 各个空格位置索引的list
	private String[] sATemp;// 字符串split()后得到的字符串数组
	private int index = 0;
	private final int SUPPMAXLEN = 10;
	private int suppMaxLen = 0;
	private String needSuppStr = "";// 右侧字符个数不足的时候，此时左侧和右侧的拼接结果
	private int needSuppLen = 0;// 右侧字符个数不足的时候，需要添加的字符的个数
	private boolean flag = false;//
	private int endIndex;
	private String corpusCodeFormat = "gbk";
	private int tempIndex = 0;
	private int leftLen = 0;
	private int rightLen = 0;
	// Logger log = LoggerFactory.getLogger(MiddleSegRawCountMapper.class);

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// startOrder和endOrder都必须是奇数
		startOrder = conf.getInt("startOrder", 0);
		endOrder = conf.getInt("endOrder", 3);

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			currentLine = new String(value.getBytes(), 0, value.getLength(), "gbk");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		currentLine = processLine(currentLine);
		// log.info("pre line---->" + preLine);
		// log.info("current line---->" + currentLine);

		currentLineLen = currentLine.length();
		if (currentLineLen <= 2 * endOrder) {
			preLine = preLine + currentLine;
			preLineLen = preLineLen + currentLineLen;
		} else {
			// log.info("currentLineList--->" + currentLineList.size());
			suppMaxLen = SUPPMAXLEN < currentLineLen ? SUPPMAXLEN : currentLineLen;
			// if (currentLineList.size() > 0) {
			for (String str : currentLineList) {
				rightSuppSub.setLength(0);
				sATemp = str.split("\t");
				needSuppStr = sATemp[0];
				// log.info("need supp-->" + str);
				currentOrder = Integer.parseInt(sATemp[1]);
				needSuppLen = Integer.parseInt(sATemp[2]);
				// log.info("suppMaxLen--->" + suppMaxLen);
				for (index = 0; index < suppMaxLen; index++) {
					cTemp = currentLine.charAt(index);
					// log.info("supp" + cTemp);
					if (cTemp != ' ' && cTemp != sepChar) {
						rightSuppSub.append(cTemp);
						needSuppLen--;
						if (needSuppLen == 0) {
							resKey.set(needSuppStr + rightSuppSub.toString());
							// log.info("after supp--->" + resKey.toString());
							context.write(resKey, ONE);
							break;
						}
					}
				}
			}
			// }
			currentLineList.clear();
			blankIndexList = sepCount(currentLine, currentLineLen);

			// int leftIndex = 0;
			// int rightIndex = 0;
			char cTemp;
			suppMaxLen = SUPPMAXLEN < preLineLen ? SUPPMAXLEN : preLineLen;
			for (currentOrder = startOrder; currentOrder <= endOrder; currentOrder += 2) {
				// log.info("current order--->" + currentOrder);
				for (int blankIndex : blankIndexList) {
					for (tempIndex = 1; tempIndex < currentOrder; tempIndex++) {

						flag = false;
						leftSb.setLength(0);
						rightSb.setLength(0);
						for (int leftIndex = blankIndex - 1; leftIndex >= 0; leftIndex--) {
							cTemp = currentLine.charAt(leftIndex);
							if (cTemp == sepChar) {
								// log.info("left出现分隔符");
								flag = true;
								break;
							}
							if (cTemp != ' ') {
								leftSb.append(cTemp);
							}
							if (leftSb.toString().length() == tempIndex) {
								// log.info("left长度满足情况");
								break;
							}
						}
						if (flag)
							continue;
						sLeft = leftSb.toString();
						sLeftLen = sLeft.length();
						if (sLeftLen != currentOrder - tempIndex) {
							if (preLineLen != 0) {
								// log.info("prelineLen---->"+preLineLen);
								endIndex = preLineLen > suppMaxLen ? (preLineLen - suppMaxLen) : 0;
								for (int leftIndex = preLineLen - 1; leftIndex > endIndex; leftIndex--) {
									// log.info("leftIndex--->"+leftIndex);
									cTemp = preLine.charAt(leftIndex);
									// log.info("向上一行扩展"+cTemp);
									if (cTemp != ' ' && cTemp != sepChar) {
										leftSb.append(cTemp);
									}
									/*
									 * if (cTemp == sepChar) { //
									 * log.info("left向上一行扩展时出现分隔符"); flag =
									 * true; break; } else if (cTemp != '
									 * '&&cTemp!=sepChar) {
									 * leftSb.append(cTemp); }
									 */
									if (leftSb.toString().length() == currentOrder - tempIndex) {
										// log.info("left向上一行扩展后长度满足要求-->"+leftSb.reverse().toString());
										break;
									}
								}
							}
						}
						if (flag)
							continue;
						sLeft = leftSb.reverse().toString();
						sLeftLen = sLeft.length();
						if (sLeftLen != currentOrder / 2)
							continue;
						for (int rightIndex = blankIndex + 1; rightIndex < currentLineLen; rightIndex++) {
							cTemp = currentLine.charAt(rightIndex);
							/*
							 * if (cTemp == sepChar) { //
							 * log.info("right出现分隔符"); flag = true; break; }
							 * else if (cTemp != ' ') { rightSb.append(cTemp); }
							 */
							if (cTemp != ' ' && cTemp != sepChar) {
								rightSb.append(cTemp);
							}
							if (rightSb.toString().length() == currentOrder / 2) {
								// log.info("right长度满足要求");
								break;
							}
						}
						if (flag)
							continue;
						sRight = rightSb.toString();
						sRightLen = sRight.length();
						if (sRightLen != currentOrder / 2) {
							currentLineList
									.add(sLeft + sRight + "\t" + currentOrder + "\t" + (currentOrder / 2 - sRightLen));
						} else {
							ngram = sLeft + sRight;
							// log.info("ngram---->" + ngram);
							resKey.set(ngram);
							context.write(resKey, ONE);
						}

					}
				}

			}
			preLine = currentLine;
			preLineLen = currentLineLen;
		}

	}

	private List<Integer> sepCount(String line, int lineLen) {
		List<Integer> indexList = new ArrayList<Integer>();
		for (int i = 0; i < lineLen; i++) {
			if (line.charAt(i) == '▲')
				indexList.add(i);
		}
		return indexList;
	}

	private String processLine(String line) {
		String posPattern = "/[a-zA-Z]{1,5}";
		String numberRegrex = "\\d+[.,]?\\d*";
		String numSign = "■";
		String sepSign = "▲";
		line = line.replaceAll(posPattern, "");
		line = line.replaceAll(numberRegrex, numSign);
		line = line.replaceAll(" ", sepSign);
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
