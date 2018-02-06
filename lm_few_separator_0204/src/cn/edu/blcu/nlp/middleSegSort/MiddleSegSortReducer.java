package cn.edu.blcu.nlp.middleSegSort;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleSegSortReducer extends Reducer<Text,Text,Text,Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text value:values){
			context.write(key, value);
		}
	}
}
