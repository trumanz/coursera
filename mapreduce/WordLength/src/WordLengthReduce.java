import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class WordLengthReduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			//StringBuilder strBuilder = new StringBuilder();
			while (values.hasNext()) {
				//strBuilder.append(values.next().toString() + "+");
				sum += values.next().get();
			}
			//Text outText = new Text();
			//outText.set(strBuilder.toString());
			
			output.collect(key, new IntWritable(sum));
		}
	}