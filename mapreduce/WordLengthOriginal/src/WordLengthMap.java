import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public  class WordLengthMap extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text wordType = new Text();
	private Text word = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
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
			output.collect(wordType, one);
		}
	}
}
