package dsa.write;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface StreamWriter{

	void stream_openFile() throws FileNotFoundException;
	void stream_writeLine(String line) throws IOException;
	void stream_close() throws IOException;
	String getType();
	
}
