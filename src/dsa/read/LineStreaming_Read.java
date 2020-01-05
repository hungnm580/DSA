package dsa.read;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LineStreaming_Read implements StreamReader{

	private String filename;
	private String line = "";
	private String typeOutput = "Line Streaming: ";
	private RandomAccessFile rand;
	
	public LineStreaming_Read(String p_filename){
		filename = p_filename;
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		rand = new RandomAccessFile(filename,"r");				
	}
	
	@Override
	public String stream_readLine() throws IOException{
		line = rand.readLine();
		return line;
	}
	
	@Override
	public boolean stream_eof(){
		if(line == null){
			return true;
		}
		else return false;
	}
	
	@Override
	public void stream_close() throws IOException{
		rand.close();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}

	public void seek(long position) throws IOException{
		rand.seek(position);			
	}
}
