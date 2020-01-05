package dsa.write;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class MMapStreaming_Write implements StreamWriter{
//	private String inputFilePath;
	private String outputFilePath;
	private String typeOutput = "MMAP Streaming: ";
	private int bufferSize; 
//	private long filesize;
	private FileChannel fileChannel;
	private MappedByteBuffer mapbuffer;
	CharBuffer charBuffer;
	CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
	
	int currentPosition = 0;
	int currentSize = 0;
	private long outputFileSize;
	
	
	public MMapStreaming_Write(String outputFilePath_, int p_bufferSize, long outputFileSize_){
//		inputFilePath = inputFilePath_;
		outputFilePath = outputFilePath_;
		bufferSize = p_bufferSize;	
		outputFileSize = outputFileSize_;
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		File output = new File(outputFilePath);
		output.delete();
		bufferSize = (int) Math.min(outputFileSize,bufferSize);
		RandomAccessFile randomAccessFile = new RandomAccessFile(output, "rw");
		fileChannel = randomAccessFile.getChannel();
		try{
			mapbuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
		}
		catch (Exception e){
			System.out.print(e.getMessage());
		}
	}
	
	@Override
	public void stream_writeLine(String line_) throws IOException{
		String line = line_;
			line += "\n";
			if(line.length()>bufferSize) {
				
				//Split the string
				int x = line.length();
				double n = (double)line.length()/(double)bufferSize;
				String[] tempString = new String[(int) Math.ceil(n)];
				for(int i = 0; i<((int) Math.ceil(n))-1; i++) {
					tempString[i] = line.substring(i*bufferSize, i*bufferSize+bufferSize);
				}
				
				tempString[((int) Math.ceil(n))-1] = line.substring(((int) Math.ceil(n)-1)*bufferSize, line.length());	
				
				//Write each parts of the string
				for(int i = 0; i<((int) Math.ceil(n)); i++) {
					mapbuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, currentSize, bufferSize);
					mapbuffer.put(tempString[i].getBytes());
					currentSize += tempString[i].length();

				}
			}
			else {
				if(mapbuffer.remaining()<(line.length())) {
					mapbuffer.clear();
					if((outputFileSize-currentSize)>line.length())
						mapbuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, currentSize, 
								Math.min(outputFileSize-currentSize, bufferSize));
					else
						mapbuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, currentSize, 
								Math.min(line.length(), bufferSize));
				}
				mapbuffer.put(line.getBytes());
				currentSize += line.length();
			}	
	}
	
	
	@Override
	public void stream_close() throws IOException{
		fileChannel.close();
		mapbuffer.clear();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}
}
