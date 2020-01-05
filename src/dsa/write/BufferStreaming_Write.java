package dsa.write;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class BufferStreaming_Write implements StreamWriter{
//	private String inputFilePath;
	private String outputFilePath;
	private FileWriter fw;
	private String typeOutput = "Buffer Streaming: ";
	private BufferedWriter buffer ;
	private int bufferSize; 
	private long filesize;
	private long outputFileSize;
	
	public BufferStreaming_Write(String outputFilePath_, int p_bufferSize, long outputFileSize_){
//		inputFilePath = inputFilePath_;
		outputFilePath = outputFilePath_;
		bufferSize = p_bufferSize;	
		outputFileSize = outputFileSize_;
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		try {
			fw = new FileWriter(outputFilePath);
			bufferSize = (int) Math.min(outputFileSize,bufferSize);
			buffer = new BufferedWriter(fw, bufferSize);
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}
	
	
	
	@Override
	public void stream_writeLine(String line) throws IOException{
		buffer.write(line + "\n");
	}
		
	@Override
	public void stream_close() throws IOException{
		buffer.close();
		fw.close();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}
}
