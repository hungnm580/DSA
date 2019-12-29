package dsa.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

public class MMapStreaming_Read implements StreamReader{
	private String filename;
	private FileReader file;
	private String line = "";
	private String typeOutput = "MMAP Streaming: ";
	private BufferedReader buffer ;
	private int bufferSize; 
	private long filesize;
	private int currentPosition = 0;
	private FileChannel fileChannel;
	private MappedByteBuffer mapbuffer;
	private char[] cb;
	private int temp_buffer;
	private int n_streams = 0;
	boolean readc = false;
	boolean first = true;
	boolean fillline = false;
	CharBuffer charBuffer;
	CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
	
	public MMapStreaming_Read(String p_filename, int p_bufferSize){
		filename = p_filename;
		bufferSize = p_bufferSize;		
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		File f = new File(filename);
		filesize = f.length();
		file = new FileReader(f);
		bufferSize = (int) Math.min(filesize,bufferSize);
		//buffer = new BufferedReader( file , bufferSize);
		RandomAccessFile randomAccessFile = new RandomAccessFile(f, "r");
		fileChannel = randomAccessFile.getChannel();
		try{
		mapbuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, 0);
		} 
		catch (Exception e){
		}
		cb = new char[bufferSize];
	}
	
	@Override
	public String stream_readLine() throws IOException{
		line = "";			
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
				mapbuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, (bufferSize * (n_streams-1)), temp_buffer);
				for (int i = 0; i<temp_buffer; i++ ){
					cb[i] = (char)mapbuffer.get();
				}
				//cb = charBuffer.array();
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
		file.close();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}
	
	@Override
	public void seek(long position) throws IOException{
	}
}
