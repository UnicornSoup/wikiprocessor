package com.github.unicornsoup.wikiprocessor.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * 
 * @author sjth
 * Aggregates the total number of revisions made by each user
 * 
 */
public class WikiHistoryReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	static final Logger log = Logger.getLogger(WikiHistoryReducer.class);
	private IntWritable result = new IntWritable();

	public void reduce(Text text, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		context.write(text, result);
	}
}
