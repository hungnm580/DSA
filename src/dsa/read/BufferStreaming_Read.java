package dsa.read;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;


public class BufferStreaming_Read implements StreamReader{

	private String filename;
	private String line = "";
	private String typeOutput = "Buffer Streaming: ";
	private int bufferSize; 
	private long filesize;
	private int currentPosition = 0;
	private char[] cb;
	private int temp_buffer;
	private int n_streams = 0;
	boolean readc = false;
	boolean first = true;
	boolean fillline = false;
	private RandomAccessFile rand;
	private FileInputStream fis;
	private BufferedInputStream bis;
	private byte[] bb;
	
	public BufferStreaming_Read(String p_filename, int p_bufferSize){
		filename = p_filename;
		bufferSize = p_bufferSize;		
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		File f = new File(filename);
		filesize = f.length();
		bufferSize = (int) Math.min(filesize,bufferSize);
		rand = new RandomAccessFile(filename,"r");
		cb = new char[bufferSize];
		bb = new byte[bufferSize];
		try {
			fis = new FileInputStream(rand.getFD());
			bis = new BufferedInputStream(fis);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Override
	public String stream_readLine() throws IOException{
		line = "";	
		//int pos = 0;
		
		while(true){
			if(readc){
				int i;
				char c;
				for(i = currentPosition;i<temp_buffer;i++){
					c = cb[i];
                    if ((c == '\n')) {
                    	if(!fillline){
                    		line = new String(cb, currentPosition, i - currentPosition);
                    	}
                    	else{
                    		line += new String(cb, currentPosition, i - currentPosition);
                    		fillline = false;
                    	}
                        currentPosition = i+1;
                        return line;
                    }
				}
				readc = false;
			}else{
				if(!first){
					if (bufferSize*(n_streams-1) + currentPosition>= filesize){
						return "";
					}
					else{
						if (fillline){
							line += new String(cb, currentPosition, temp_buffer - currentPosition);
						}
						else{
							line = new String(cb, currentPosition, temp_buffer - currentPosition);
						}
						fillline = true;
					}
				}
				else{
					first = false;
				}
				temp_buffer = (int)Math.min(filesize - bufferSize*n_streams , bufferSize);
				currentPosition = 0;
				n_streams++;
				bis.read(bb);
				for (int i = 0; i<temp_buffer; i++ ){
					cb[i] = (char)bb[i];
				}
				readc = true;
			} 
		}
	}
	
	@Override
	public boolean stream_eof(){
		if(line == ""){
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
	
	@Override
	public void seek(long position) throws IOException{
		int n_streams_new = (int)(position/bufferSize);
		currentPosition = (int)position % bufferSize;
		if (n_streams_new+1 != n_streams){
			rand.seek(n_streams_new*bufferSize);
			n_streams = n_streams_new;
			first = false;
			temp_buffer = (int)Math.min(filesize - bufferSize*n_streams , bufferSize);
			bis = new BufferedInputStream(fis);
			bis.read(bb);
			for (int i = 0; i<temp_buffer; i++ ){
				cb[i] = (char)bb[i];
			}
			n_streams ++;
			readc = true;
		}
	}
}
