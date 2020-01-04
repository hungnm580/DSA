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

public class MMapStreaming_Write implements StreamWriter{
	private String inputFilePath;
	private String outputFilePath;
	private String typeOutput = "MMAP Streaming: ";
	private int bufferSize; 
	private long filesize;
	private FileChannel fileChannel;
	private MappedByteBuffer mapbuffer;
	CharBuffer charBuffer;
	CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
	
	int currentPosition = 0;
	int currentSize = 0;
	
	
	public MMapStreaming_Write(String inputFilePath_, String outputFilePath_, int p_bufferSize){
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
		
		RandomAccessFile randomAccessFile = new RandomAccessFile(output, "rw");

		fileChannel = randomAccessFile.getChannel();
		try{
			mapbuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
		}
		catch (Exception e){
		}
	}
	
	@Override
	public void stream_writeLine(String line_) throws IOException{
		currentPosition = mapbuffer.position();
		if(mapbuffer.remaining()<(line_.getBytes().length+"\n".getBytes().length)) {
			currentPosition = mapbuffer.position();
			currentSize += currentPosition;
			mapbuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, currentSize, Math.min(filesize-currentSize,bufferSize));
		}
		mapbuffer.put(line_.getBytes());
		mapbuffer.put("\n".getBytes());
	}
	
	
	@Override
	public void stream_close() throws IOException{
		fileChannel.close();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}
}
