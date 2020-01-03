package dsa.write;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

public class LineStreaming_Write implements StreamWriter{

	private String filename;
	private FileWriter writer;
	private String typeOutput = "Line Streaming: ";
	private BufferedWriter buffer ;
	
	public LineStreaming_Write(String p_filename){
		filename = p_filename;
	}
	
	@Override
	public void stream_openFile() throws FileNotFoundException{
		try {
			writer = new FileWriter(filename);
			buffer = new BufferedWriter(writer,  100 * 1024);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void stream_writeLine(String line) throws IOException{
		buffer.write(line);
	}	
	
	@Override
	public void stream_close() throws IOException{
		buffer.close();
		writer.close();
	}
	
	@Override
	public String getType(){
		return typeOutput;
	}	
}
