package dsa.experiment;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;


import dsa.read.LineStreaming_Read;
import dsa.write.*;
import org.apache.commons.codec.digest.DigestUtils;

public class Experiments_Write {
	private String inputFilePath;
	private String outputFilePath;
	
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
				writer = new MMapStreaming_Write(inputFilePath, outputFilePath, bufferSize);
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
	
	public static boolean checkSum(String inputFilePath, String outputFilePath) {
		String inputMD5 = "input";
		String ouputMD5 = "output";
		try {
			inputMD5 = DigestUtils.md5Hex(new FileInputStream(inputFilePath));
			ouputMD5 = DigestUtils.md5Hex(new FileInputStream(outputFilePath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		  return inputMD5.equals(ouputMD5);
		  	

	}
	
	public static void main(String[] args) {
		
		String inputFilePath = "/home/hungnm/cast_info.csv";
		String outputFilePath = "/home/hungnm/testout.txt";

		int numsimulations = 1;
		long[][] simulations = new long[4][numsimulations];
		for (int i = 0 ; i< numsimulations; i++){
			Experiments_Write exp = new Experiments_Write(inputFilePath, outputFilePath);
			try {
				//Clear cache
				String[] cmd = { "sh", "/home/hungnm/clearCache.sh"};
				Process p = Runtime.getRuntime().exec(cmd); 
				
				simulations[0][i] = exp.SequentialStreaming_Write(1,0);
				p = Runtime.getRuntime().exec(cmd); 

				simulations[1][i] = exp.SequentialStreaming_Write(2,0);
				p = Runtime.getRuntime().exec(cmd); 

				simulations[2][i] = exp.SequentialStreaming_Write(3,2048);
				p = Runtime.getRuntime().exec(cmd); 

				simulations[3][i] = exp.SequentialStreaming_Write(4,2048*2048);
				p = Runtime.getRuntime().exec(cmd); 


//				p = Runtime.getRuntime().exec(cmd);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		System.out.print(checkSum(inputFilePath, outputFilePath));
	}
	
}
