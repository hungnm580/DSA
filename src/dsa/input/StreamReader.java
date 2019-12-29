package dsa.input;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface StreamReader{

	void stream_openFile() throws FileNotFoundException;
	String stream_readLine() throws IOException;
	boolean stream_eof();
	void stream_close() throws IOException;
	String getType();
	void seek(long position) throws IOException;
}
