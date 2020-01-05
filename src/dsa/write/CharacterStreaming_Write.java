package dsa.write;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileWriter;  

public class CharacterStreaming_Write implements StreamWriter{

	private String filename;
	private FileWriter writer;
	private String line = "";
	private String typeOutput = "Character Streaming: ";
	
	public CharacterStreaming_Write(String p_filename){
		filename = p_filename;
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		try {
			writer = new FileWriter(new File(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void stream_writeLine(String line_) throws IOException{
		line = line_;
		if(line==null)
			return;
		char c;
		for(int i=0; i<line.length(); i++) {
			c = line.charAt(i);
			writer.write(c);
		}
		writer.write("\n");
	}
	
	@Override
	public void stream_close() throws IOException{
		writer.close();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}	
}
