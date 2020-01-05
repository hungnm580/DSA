package dsa.read;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

public class CharacterStreaming_Read implements StreamReader{

	private String filename;
	//private FileReader file;
	private String line = "";
	private String typeOutput = "Character Streaming: ";
	private RandomAccessFile file;
	
	public CharacterStreaming_Read(String p_filename){
		filename = p_filename;
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		//file = new FileReader(new File(filename));
		file = new RandomAccessFile(filename,"r");
	}
	
	@Override
	public String stream_readLine() throws IOException{
		line = "";
		int c;
		while((c = file.read())!=-1){
			if(c == '\n'){
				return line ;
			}
			line += (char)c;
		}
		return line;
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
		file.seek(position);
		//stream_close();
		//stream_openFile();
		//file.skip(position);
	}

	
}
