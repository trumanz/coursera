import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordLength {
	
	private static void GenerateInput(String localFilePath, Path toHDFSDir, int contRepeat, int numOfFiles) throws IOException{
		FileChannel inChannle  = FileChannel.open(FileSystems.getDefault().getPath(localFilePath));
		FileSystem fs = FileSystem.get(toHDFSDir.toUri(), new JobConf(WordLength.class));
		ByteBuffer buf =  ByteBuffer.allocate(1*1024*1024);
		FSDataOutputStream outStream = null;

		try {
			while(numOfFiles-- > 0){
				outStream = fs.create(new Path(toHDFSDir, Integer.toString(numOfFiles)));
				while(contRepeat-- > 0){
					inChannle.position(0);
					while(inChannle.read(buf) > 0){
						buf.flip();
						outStream.write(buf.array());
						buf.clear();
					}
				}
				outStream.close();
			}
		} catch (IOException e){
			throw e;
		} finally {
			inChannle.close();
			//fs.close(); it's get a glocal instance do not close it!
			if(outStream != null) outStream.close();
		}
	}
	
	private static void RunMapReduce(Path input, Path output) throws IOException{
		JobConf conf = new JobConf(WordLength.class);
		conf.setJobName("wordlength");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordLengthMap.class);
		conf.setReducerClass(WordLengthReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, output);
		
		RunningJob rj =  JobClient.runJob(conf);
		rj.waitForCompletion();
	}
	
	
	public static void main(String[] args) throws Exception {
	    try{
	    	
	    	Path inputPath =  new Path("hdfs://localhost:54310/input");
	    	Path outputPath = new Path("hdfs://localhost:54310/output");
	    	String localFile = "/home/hduser/workspace/coursera-datasci/mapreduce/WordLength/data/document.txt";
	    				
	        //1. rebuild input,output
	    	FileSystem fs = FileSystem.get(inputPath.toUri(), new JobConf(WordLength.class));
	    	fs.delete(inputPath, true);
	    	fs.delete(outputPath, true);
	    	fs.mkdirs(inputPath);    	
	    	GenerateInput(localFile, inputPath, 1,1);
	    	
	    	//2. run task
	    	RunMapReduce(inputPath, outputPath);
	    	
	    	RemoteIterator<LocatedFileStatus> outputs  =  fs.listFiles(outputPath, true);
			while(outputs.hasNext()){
				LocatedFileStatus fstatus = outputs.next();
				if(fstatus.isFile()){
					System.out.println(fstatus.getPath());
					FSDataInputStream  fin = fs.open(fstatus.getPath());
					ByteBuffer buf = ByteBuffer.allocate(1*1024*1024);//1M buffer
					while( fin.read(buf) > 0){
						buf.flip();
						String str = Charset.forName("UTF-8").newDecoder().decode(buf).toString();
						System.out.print(str);
						buf.clear();
					}
					fin.close();
				}
			}
	    	
	    	
	    } catch(Exception e){
	    	System.out.println(e.getMessage());
	    	e.printStackTrace();
	    }
		
		
		
		
		
	}
}