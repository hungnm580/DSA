package dsa.write;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;


public class BufferStreaming_Write implements StreamWriter{
	private String inputFilePath;
	private String outputFilePath;
	private FileWriter fw;
	private String line = "";
	private String typeOutput = "Buffer Streaming: ";
	private BufferedWriter buffer ;
	private int bufferSize; 
	private long filesize;
	
	public BufferStreaming_Write(String inputFilePath_, String outputFilePath_, int p_bufferSize){
		inputFilePath = inputFilePath_;
		outputFilePath = outputFilePath_;
		bufferSize = p_bufferSize;		
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		File input = new File(inputFilePath);
		File output = new File(outputFilePath);
		filesize = input.length();
		bufferSize = (int) Math.min(filesize,bufferSize);
		
		try {
			fw = new FileWriter(output);
			buffer = new BufferedWriter(fw, bufferSize);
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}
	
	@Override
	public void stream_writeLine(String line_) throws IOException{
		buffer.write(line);
	}
		
	@Override
	public void stream_close() throws IOException{
		fw.close();
		buffer.close();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}
}
