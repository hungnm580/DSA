package dsa.experiment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import dsa.read.BufferStreaming_Read;
import dsa.read.CharacterStreaming_Read;
import dsa.read.LineStreaming_Read;
import dsa.read.MMapStreaming_Read;
import dsa.read.StreamReader;
import dsa.write.*;

public class Experiments_Write {
	private String inputFilePath;
	private String outputFilePath;
	private long[] randomNumbers;
	
	public Experiments_Write(String p_inputFilePath_, String p_outputFilePath_){
		inputFilePath=p_inputFilePath_;
		outputFilePath=p_outputFilePath_;
	}
	
	public long SequentialStreaming_Write(int type, int bufferSize) throws IOException{
		String line="";
		LineStreaming_Read reader = new LineStreaming_Read(inputFilePath);
		StreamWriter writer;

		try {
			reader.stream_openFile();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();}
			
		
		switch(type){
			case 1:
				writer = new CharacterStreaming_Write(outputFilePath);

				break;
			case 2:
				writer = new LineStreaming_Write(outputFilePath);
				break;
			case 3:
				writer = new BufferStreaming_Write(inputFilePath, outputFilePath, bufferSize);
				break;
			case 4:
				writer = new CharacterStreaming_Write(outputFilePath);
				break;	
			default:
				writer = new CharacterStreaming_Write(outputFilePath);
		}
		
		System.out.println("Type - "+ writer.getType());
		long startTime = System.currentTimeMillis();
		
		writer.stream_openFile();
		
		//Read line by line
		do {
			try{    
				line = reader.stream_readLine();
				if(line!=null)
		           writer.stream_writeLine(line);    
		    }catch(Exception e){System.out.println(e);} 
		     }
		while(line!=null);
		
		//Close all the streams
		writer.stream_close();
		reader.stream_close();
		
		long endTime = System.currentTimeMillis();
		long timeElapsed = endTime - startTime;
		System.out.printf("Execution time in milliseconds: %d \n" , timeElapsed);
		
		return timeElapsed;
	}
	
	public long RandomJumpStreaming_Read(int type, int bufferSize) throws IOException{
				
		StreamReader input;
		
		switch(type){
			case 1:
				input = new CharacterStreaming_Read(inputFilePath);
				break;
			case 2:
				input = new LineStreaming_Read(inputFilePath);
				break;
			case 3:
				input = new BufferStreaming_Read(inputFilePath, bufferSize);
				break;
			case 4:
				input = new MMapStreaming_Read(inputFilePath, bufferSize);
				break;	
			default:
				input = new CharacterStreaming_Read(inputFilePath);
		}
		
		System.out.println("Type - "+ input.getType());
		long startTime = System.currentTimeMillis();
		
		input.stream_openFile();
		
		for(int i =0;i<randomNumbers.length;i++){
			input.seek(randomNumbers[i]);
			
			String line = input.stream_readLine();
			//System.out.println("Line : " + line);
		}
		
		input.stream_close();
		
		long endTime = System.currentTimeMillis();
		long timeElapsed = endTime - startTime;
		
		System.out.printf("Execution time in milliseconds: %d \n" , timeElapsed);
		
		return timeElapsed;
	}
	
	public void generateRandomTests(int numIterations){
		File f = new File(inputFilePath);
		int filesize = (int)f.length();
		
		randomNumbers = new long[numIterations];
		
		for (int i=0;i<numIterations;i++){
			Random rand = new Random(); 
			randomNumbers[i] = rand.nextInt(filesize);
		}
	}
	
	public static void main(String[] args) {
		
		String inputFilePath = "/home/hungnm/cast_info.csv";
		String outputFilePath = "/home/hungnm/testout.txt";

		int numsimulations = 5;
		long[][] simulations = new long[4][numsimulations];
		for (int i = 0 ; i< numsimulations; i++){
			Experiments_Write exp = new Experiments_Write(inputFilePath, outputFilePath);
			try {
//				simulations[0][i] = exp.SequentialStreaming_Write(1,0);
//				simulations[1][i] = exp.SequentialStreaming_Write(2,0);
				simulations[1][i] = exp.SequentialStreaming_Write(3,100000000);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
	}
	
}
