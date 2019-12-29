package dsa.experiment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import dsa.input.BufferStreaming_Read;
import dsa.input.CharacterStreaming_Read;
import dsa.input.LineStreaming_Read;
import dsa.input.MMapStreaming_Read;
import dsa.input.StreamReader;

public class Experiments {
	private String filename;
	private long[] randomNumbers;
	
	public Experiments(String p_filename){
		filename=p_filename;
	}
	
	public long SequentialStreaming_Read(int type, int bufferSize) throws IOException{
		
		StreamReader input;
		
		switch(type){
			case 1:
				input = new CharacterStreaming_Read(filename);
				break;
			case 2:
				input = new LineStreaming_Read(filename);
				break;
			case 3:
				input = new BufferStreaming_Read(filename, bufferSize);
				break;
			case 4:
				input = new MMapStreaming_Read(filename, bufferSize);
				break;	
			default:
				input = new CharacterStreaming_Read(filename);
		}
		
		
		
		System.out.println("Type - "+ input.getType());
		long startTime = System.currentTimeMillis();
		
		input.stream_openFile();
		int counter = 0;
		while(true){
			String line = input.stream_readLine();
			if (input.stream_eof()){
				break;
			}
			counter += line.length();
		}
		input.stream_close();
		
		long endTime = System.currentTimeMillis();
		long timeElapsed = endTime - startTime;
		System.out.println("Number of Bytes Read : " + counter);
		System.out.printf("Execution time in milliseconds: %d \n" , timeElapsed);
		
		return timeElapsed;
	}
	
	public long RandomJumpStreaming_Read(int type, int bufferSize) throws IOException{
				
		StreamReader input;
		
		switch(type){
			case 1:
				input = new CharacterStreaming_Read(filename);
				break;
			case 2:
				input = new LineStreaming_Read(filename);
				break;
			case 3:
				input = new BufferStreaming_Read(filename, bufferSize);
				break;
			case 4:
				input = new MMapStreaming_Read(filename, bufferSize);
				break;	
			default:
				input = new CharacterStreaming_Read(filename);
		}
		
		System.out.println("Type - "+ input.getType());
		long startTime = System.currentTimeMillis();
		
		input.stream_openFile();
		int counter = 0;
		
		for(int i =0;i<randomNumbers.length;i++){
			input.seek(randomNumbers[i]);
			
			String line = input.stream_readLine();
			//System.out.println("Line : " + line);
			counter += line.length();
		}
		
		input.stream_close();
		
		long endTime = System.currentTimeMillis();
		long timeElapsed = endTime - startTime;
		System.out.println("Number of Bytes Read : " + counter);
		System.out.printf("Execution time in milliseconds: %d \n" , timeElapsed);
		
		return timeElapsed;
	}
	
	public void generateRandomTests(int numIterations){
		File f = new File(filename);
		int filesize = (int)f.length();
		
		randomNumbers = new long[numIterations];
		
		for (int i=0;i<numIterations;i++){
			Random rand = new Random(); 
			randomNumbers[i] = rand.nextInt(filesize);
		}
	}
	
}
