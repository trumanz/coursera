import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper;


public  class WordLengthMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text wordType = new Text();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			int len = token.length();
			if (len >= 10) {
				wordType.set("Big");
			} else if (len >= 5) {
				wordType.set("Medium");
			} else if (len >= 2) {
				wordType.set("Small");
			} else {
				wordType.set("Tiny");
			}
			word.set(token);
			context.write(wordType, one);
		}
	}
}
