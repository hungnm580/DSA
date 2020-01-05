package dsa.main;

import java.io.IOException;

import dsa.experiment.Experiments;

public class main {
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String filename = "G:\\Documentos\\MasterDegree\\BDMA\\" +
				"Classes\\Data system Arquitectures\\Project\\Data\\cast_info.csv";
		//cast_info
		int numsimulations = 5;
		long[][] simulations = new long[4][numsimulations];
		for (int i = 0 ; i< numsimulations; i++){
			Experiments exp = new Experiments(filename);
			simulations[0][i] = exp.SequentialStreaming_Read(1,0);
			simulations[1][i] = exp.SequentialStreaming_Read(2,0);
			simulations[2][i] = exp.SequentialStreaming_Read(3,8192);
			simulations[3][i] = exp.SequentialStreaming_Read(4,10000000);
		}
		
	}
}
