package dsa.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;


import dsa.read.LineStreaming_Read;
import dsa.read.MMapStreaming_Read;
import dsa.write.*;
public class RRMerge {
	//Number of input files
	private static int numFiles = 5;
	
	//Input and output files
	private static String folder = "C:\\Users\\Hung\\Downloads\\imdb\\";
	private static String[] files = {"movie_link.csv","keyword.csv","aka_name.csv","person_info.csv","cast_info.csv"};
	private static String outputFilePath[] = {"characterWriting.csv", "lineWriting.csv", "bufferedWriting.csv", "mmapWriting.csv"};
	
	//Readers array
	MMapStreaming_Read[] readers = new MMapStreaming_Read[numFiles];

	public RRMerge(){
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
	
	public static void main(String[] args) {	
		//Multiple tests to get an average time
		int numsimulations = 1;
		
		//Different buffer sizes.
		int[] bufferSizeArray = {1024,4096,8192,16384,65536,524288,4194304,10000000};  
		long simulations = 0;
		
		//Readers type
		String readersType = "MMapReading";
		
		//Save times of execution in results.csv
		try {
		File resultsFile =new File(folder + "results.csv");
	    BufferedWriter writer = new BufferedWriter(new FileWriter(resultsFile));
	    writer.write("Simulation, Reading type, Writing type, Buffer size, Execution time\n");
	    //Print input files names
	    System.out.println(files);
	    
		for (int i = 0 ; i< numsimulations; i++){
			//RRMerge Experiment 
			RRMerge exp = new RRMerge();
			
			//Stream by Character
			simulations = exp.SequentialStreaming_Write(1,0);
			writer.write(i + "," + readersType + "," + "CharacterWriting" + "," + "0" + "," + simulations + '\n');
			
			//Line Stream
			simulations = exp.SequentialStreaming_Write(2,0);
			writer.write(i + "," + readersType + "," + "LineWriting" + "," + "0" + "," + simulations + '\n');
			
			//Different versions of buffer Size for line and MMAP streaming
			for(int j = 0; j<bufferSizeArray.length;j++){
				//Buffered Stream
				simulations = exp.SequentialStreaming_Write(3,bufferSizeArray[j]);
				writer.write(i + "," + readersType + "," + "BufferedWriting" + "," + bufferSizeArray[j] + "," + simulations + '\n');
				
				//MMAP Stream
				simulations = exp.SequentialStreaming_Write(4,bufferSizeArray[j]);
				writer.write(i + "," + readersType + "," + "MMapWriting" + "," + bufferSizeArray[j] + "," + simulations + '\n');
			}	
		}
			writer.close();
		} catch(IOException e) {
			System.out.print(e.getMessage());
		}				
	}	
}
