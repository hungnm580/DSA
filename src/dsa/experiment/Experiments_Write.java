package dsa.experiment;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;


import dsa.read.LineStreaming_Read;
import dsa.read.MMapStreaming_Read;
import dsa.write.*;
import org.apache.commons.codec.digest.DigestUtils;

public class Experiments_Write {
	private static String inputFilePath;
	private static String outputFilePath[] = {"characterWriting.csv", "lineWriting.csv", "bufferedWriting.csv", "mmapWriting.csv"};

	private String folder = "C:\\Users\\Hung\\Downloads\\imdb\\";
	private String[] files = {"movie_link.csv","keyword.csv","aka_name.csv","person_info.csv","cast_info.csv"};
		
//	//Multiple tests to get an average time
//	private static int numsimulations = 5;
//	
//	//Different buffer sizes.
//	int[] bufferSizeArray = {1024,4096,8192,16384,65536,524288};  
//			
	//Number of files
	private int numFiles = 5;
	
	private static long outputFileSize = 0;
	
	//Readers array
	private	MMapStreaming_Read[] readers = new MMapStreaming_Read[numFiles];

	
	public Experiments_Write(String[] files_t, String folder_t){
		files = files_t;		
		folder = folder_t;
		numFiles = files.length;
	}
	
	public long SequentialStreaming_Write(int type, int bufferSize) throws IOException{
		String line="";
		
		//Open all readers
		for(int i=0; i<numFiles; i++) {
			File f = new File(folder+files[i]);
			readers[i] = new MMapStreaming_Read(folder+files[i], 10000000);
			readers[i].stream_openFile();
			
		}
		
		//Caculate ouput file size for mmap
		long outputFileSize = 0;
		if(type==4||type==3) {
			outputFileSize = 0;
			for(int i=0; i<numFiles; i++) {
				File f = new File(folder+files[i]);
				outputFileSize += f.length();			
			}
		}
		
		//Open a writer
		StreamWriter writer;
		switch(type){
			case 1:
				writer = new CharacterStreaming_Write(folder+outputFilePath[0]);
				break;
			case 2:
				writer = new LineStreaming_Write(folder+outputFilePath[1]);
				break;
			case 3:
				writer = new BufferStreaming_Write(folder+outputFilePath[2], bufferSize, outputFileSize);
				break;
			case 4:
				writer = new MMapStreaming_Write(folder+outputFilePath[3], bufferSize, outputFileSize);
				break;	
			default:
				writer = new CharacterStreaming_Write(folder+outputFilePath[0]);
		}
		
		//Get start time
		System.out.println("Type - "+ writer.getType());
		long startTime = System.currentTimeMillis();
		
		//Open writing stream
		writer.stream_openFile();
		
		// Flags array
		boolean[] flags = new boolean[numFiles];
		for(int i=0; i<numFiles; i++) {
			flags[i] = true;
		}
		
		//Merge all files
		boolean stop = false;
		while(!stop) {
			
			for(int i=0; i<numFiles; i++) {
				if(flags[i]) {
					line = readers[i].stream_readLine();
			        writer.stream_writeLine(line); 
			        flags[i]=!readers[i].stream_eof();
				}
			}		
			stop=true;
			for(int i=0; i<numFiles; i++) {
				if(flags[i]) {
					stop = false;
				}
			}
		}
		
		//Close all the streams
		writer.stream_close();
		for(int i=0; i<numFiles; i++) {
			readers[i].stream_close();
		}
		
		//Get end time
		long endTime = System.currentTimeMillis();
		long timeElapsed = endTime - startTime;
		System.out.printf("Execution time in milliseconds: %d \n" , timeElapsed);
		
		return timeElapsed;
	}
	
	
}
