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
	
	private static void GenerateInput(String localFilePath, Path toHDFSDir, int contRepeat, int numOfFiles){
		FileChannel inChannle  = FileChannel.open(FileSystems.getDefault().getPath(local_from));
		FileSystem fs = FileSystem.get(toHDFSDir.toUri(), new JobConf(WordLength.class));
		ByteBuffer buf =  ByteBuffer.allocate(1*1024*1024);
		FSDataOutputStream outStream = null;

		try {
			while(numOfFiles-- > 0){
				outStream = fs.create(toHDFSDir);
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
			fs.close();
			if(outStream != null) outStream.close();
		}
	}
	
	private static void TestMapReduce(String input, String output){
		JobConf conf = new JobConf(WordLength.class);
		conf.setJobName("wordlength");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(WordLengthMap.class);
		conf.setReducerClass(WordLengthReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		Path outpath = new Path(output);
		System.out.println("try to delete output path if it exist " + outpath.toUri());
		FileSystem fs = FileSystem.get(outpath.toUri(), conf);
		fs.delete(outpath, true);//Now, no exception even the path not exist

		RunningJob rj =  JobClient.runJob(conf);
		

		rj.waitForCompletion();
	
		
		RemoteIterator<LocatedFileStatus> outputs  =  fs.listFiles(outpath, true);
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
	}
	
	
	public static void main(String[] args) throws Exception {
	    try{
	    	
	    	Path inputPath =  new Path("hdfs://localhost:54310/input");
	    	Path outputPath = new Path("hdfs://localhost:54310/output");
	    	String localFile = "/home/hduser/workspace/coursera/datasci/mapreduce/WordLength/src/DocumentCreater.java";
	    				
	        //1. rebuild input,output
	    	FileSystem fs = FileSystem.get(inputPath.toUri(), new JobConf(WordLength.class));
	    	fs.delete(inputPath, true);
	    	fs.delete(outputPath, true);
	    	fs.mkdirs(inputPath);
	    	fs.mkdirs(outputPath);	    	
	    	GenerateInput(localFile, inputPath, 1,1);
	    	
	    	TestMapReduce();
	    } catch(Exception e){
	    	System.out.println(e.getMessage());
	    	e.printStackTrace();
	    }
		
		
		
		
		
	}
}