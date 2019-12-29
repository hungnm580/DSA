package dsa.main;

import java.io.IOException;

import dsa.experiment.Experiments;

public class randjump {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		String folder = "G:\\Documentos\\MasterDegree\\BDMA\\" +
				"Classes\\Data system Arquitectures\\Project\\Data\\";
		String[] inputfiles = {"cast_info.csv"};//{"movie_link.csv","keyword.csv"};//,"aka_name.csv","person_info.csv","cast_info.csv"};
		
		//cast_info
		int numsimulations = 5;
		int[] bufferSizeArray = {10,128,1024,4096,8192,16384,65536,524288,4194304,10000000};  
		long simulations = 0;
		int numberRandomIterations = 10;
		
		for (String f:inputfiles){
			String filename = folder + f;
			Experiments exp = new Experiments(filename);
			exp.generateRandomTests(numberRandomIterations);
			simulations = exp.RandomJumpStreaming_Read(1,0);
			simulations = exp.RandomJumpStreaming_Read(2,0);
			
		}

	}

}
