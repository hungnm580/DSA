package dsa.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import dsa.experiment.Experiments_Write;

public class rrmerge {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String outputFilePath[] = {"characterWriting.csv", "lineWriting.csv", "bufferedWriting.csv", "mmapWriting.csv"};

		String folder = "G:\\Documentos\\MasterDegree\\BDMA\\" +
				"Classes\\Data system Arquitectures\\Project\\Data\\";
		String[] files = {"movie_link.csv","keyword.csv","aka_name.csv","person_info.csv","cast_info.csv"};
		
		int numsimulations = 5;
		
		//Different buffer sizes.
		int[] bufferSizeArray = {1024,4096,8192,16384,65536,524288,4194304,10000000};  
		long simulations = 0;
		
		//Readers type
		String readersType = "MMapReading";
		
		//Save times of execution in results.csv
		try {
		File resultsFile =new File(folder + "results.csv");
	    BufferedWriter writer = new BufferedWriter(new FileWriter(resultsFile));
	    writer.write("Simulation, Reading type, Writing type, Buffer size, Execution time\n");
	    //Print input files names
	    System.out.println(files);
	    
		for (int i = 0 ; i< numsimulations; i++){
			//RRMerge Experiment 
			Experiments_Write exp = new Experiments_Write(files, folder);
			
			//Stream by Character
			simulations = exp.SequentialStreaming_Write(1,0);
			writer.write(i + "," + readersType + "," + "CharacterWriting" + "," + "0" + "," + simulations + '\n');
			
			//Line Stream
			simulations = exp.SequentialStreaming_Write(2,0);
			writer.write(i + "," + readersType + "," + "LineWriting" + "," + "0" + "," + simulations + '\n');
			
			//Different versions of buffer Size for line and MMAP streaming
			for(int j = 0; j<bufferSizeArray.length;j++){
				//Buffered Stream
				simulations = exp.SequentialStreaming_Write(3,bufferSizeArray[j]);
				writer.write(i + "," + readersType + "," + "BufferedWriting" + "," + bufferSizeArray[j] + "," + simulations + '\n');
				
				//MMAP Stream
				simulations = exp.SequentialStreaming_Write(4,bufferSizeArray[j]);
				writer.write(i + "," + readersType + "," + "MMapWriting" + "," + bufferSizeArray[j] + "," + simulations + '\n');
			}	
		}
			writer.close();
		} catch(IOException e) {
			System.out.print(e.getMessage());
		}				

	}

}
