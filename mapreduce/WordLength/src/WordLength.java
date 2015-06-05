import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class WordLength {
	
	private static void GenerateInput(String localFilePath, Path toHDFSDir, int contRepeat, int numOfFiles) throws IOException{
		FileChannel inChannle  = FileChannel.open(FileSystems.getDefault().getPath(localFilePath));
		FileSystem fs = FileSystem.get(toHDFSDir.toUri(), new Configuration());
		ByteBuffer buf =  ByteBuffer.allocate(1*1024*1024);
		FSDataOutputStream outStream = null;
	
		System.out.println("create contRepeat=" + contRepeat +", numOfFiles="+numOfFiles);
		try {
			//while(numOfFiles-- > 0){
				//System.out.println("file " + numOfFiles);
				Path outPath = new Path(toHDFSDir, Integer.toString(numOfFiles));
				//System.out.println("file " + outPath.toString());
				outStream = fs.create(outPath);
				int repeat = contRepeat;
				while(repeat-- > 0){
					//System.out.println("repeat " + repeat);
					inChannle.position(0);
					while(inChannle.read(buf) > 0){
						buf.flip();
						outStream.writeBytes(Charset.forName("UTF-8").newDecoder().decode(buf).toString());
						buf.clear();
					}
				}
				outStream.close();
				outStream= null;
			//}
		    while(--numOfFiles > 0){
		    	FileUtil.copy(fs, outPath, fs, new Path(toHDFSDir, Integer.toString(numOfFiles)), false, true, new Configuration());
		    }
		} catch (IOException e){
			throw e;
		} finally {
			inChannle.close();
			//fs.close(); it's get a glocal instance do not close it!
			if(outStream != null) outStream.close();
		}
	}
	
	private static void RunMapReduce(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException{
		
		
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "wordLength");
	    job.setJarByClass(WordLength.class);
	    
	    job.setMapperClass(WordLengthMap.class);
	    job.setCombinerClass(WordLengthReduce.class);
	    
	    job.setReducerClass(WordLengthReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	 
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	
	    
	    job.waitForCompletion(true);
		
		
	}
	
	
	public static void main(String[] args) throws Exception {
	    try{
	    	
	    	Path inputPath =  new Path("hdfs://localhost:54310/input");
	    	Path outputPath = new Path("hdfs://localhost:54310/output");
	    	String localFile = "/home/hduser/workspace/coursera-datasci/mapreduce/WordLength/data/document.txt";
	    				
	        //1. rebuild input,output
	    	FileSystem fs = FileSystem.get(inputPath.toUri(), new Configuration());
	    	fs.delete(inputPath, true);
	    	fs.delete(outputPath, true);
	    	fs.mkdirs(inputPath);    	
	    	GenerateInput(localFile, inputPath, 1,1);
	    	
	    	//2. run task
	    	long start_ms = System.currentTimeMillis();
	    	RunMapReduce(inputPath, outputPath);
	    	start_ms = System.currentTimeMillis() - start_ms;
	    	System.out.println("use time " + start_ms);
	    	System.exit(1);
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