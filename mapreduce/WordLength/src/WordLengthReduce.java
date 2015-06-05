import java.io.IOException;



import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;


public class WordLengthReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			//StringBuilder strBuilder = new StringBuilder();
			for(IntWritable val: values){
				sum += val.get();
			}
			//Text outText = new Text();
			//outText.set(strBuilder.toString());
			System.out.println("^^^ezoucai: " + key.toString() +  Integer.toString(sum) );
			context.write(key, new IntWritable(sum));
		}
	}