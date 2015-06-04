import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class WordLengthReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder strBuilder = new StringBuilder();
			while (values.hasNext()) {
				strBuilder.append(values.next().toString() + "+");
			}
			Text outText = new Text();
			outText.set(strBuilder.toString());
			
			output.collect(key, outText);
		}
	}