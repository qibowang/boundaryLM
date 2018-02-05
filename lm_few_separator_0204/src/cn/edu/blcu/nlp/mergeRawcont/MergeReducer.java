package cn.edu.blcu.nlp.mergeRawcont;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
	private LongWritable resValue = new LongWritable();
	@Override
	protected void reduce(Text ngram, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long rawCountSum = 0l;
		for(LongWritable value:values){
			rawCountSum += value.get();
		}
		resValue.set(rawCountSum);
		context.write(ngram, resValue);
	}

}
