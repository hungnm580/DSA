package dsa.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import dsa.experiment.Experiments_Read;

public class length {
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String folder = "G:\\Documentos\\MasterDegree\\BDMA\\" +
				"Classes\\Data system Arquitectures\\Project\\Data\\";
		String[] inputfiles = {"movie_link.csv","keyword.csv","aka_name.csv","person_info.csv","cast_info.csv"};

		//Multiple tests to get an average time
		int numsimulations = 5;
		//Different buffer sizes.
		int[] bufferSizeArray = {10,128,1024,4096,8192,16384,65536,524288,4194304,10000000};  
		long simulations = 0;
		
		//Save times of execution in results.csv
		File resultsFile =new File(folder + "results_2.csv");
	    BufferedWriter writer = new BufferedWriter(new FileWriter(resultsFile));
	    writer.write("file,simulation,type,buffersize,time\n");
	    
	    //Sequential experimentation
		for (String f:inputfiles){
			String filename = folder + f;
			for (int i = 0 ; i< numsimulations; i++){
				//Experiment container
				Experiments_Read exp = new Experiments_Read(filename);
				//Stream by Character
				simulations = exp.SequentialStreaming_Read(1,0);
				writer.write(f + "," + i + "," + "RL_Character" + "," + "0" + "," + simulations + '\n');
				//Unbuffered Stream
				simulations = exp.SequentialStreaming_Read(2,0);
				writer.write(f + "," + i + "," + "RL_NoBuffered" + "," + "0" + "," + simulations + '\n');
				//Different versions of buffer Size for buffered and MMAP streaming
				for(int j = 0; j<bufferSizeArray.length;j++){
					//Buffered Stream
					simulations = exp.SequentialStreaming_Read(3,bufferSizeArray[j]);
					writer.write(f + "," + i + "," + "RL_Buffered" + "," + bufferSizeArray[j] + "," + simulations + '\n');
					//MMAP Stream
					simulations = exp.SequentialStreaming_Read(4,bufferSizeArray[j]);
					writer.write(f + "," + i + "," + "RL_MMAP" + "," + bufferSizeArray[j] + "," + simulations + '\n');
				}	
			}
		}
		writer.close();
	}
}
