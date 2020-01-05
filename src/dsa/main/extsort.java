package dsa.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import dsa.experiment.Experiments_Read;
import dsa.experiment.Experiments_MergeSort;

public class extsort {

	public static void main(String[] args) throws IOException {
	
		String folder = "G:\\Documentos\\MasterDegree\\BDMA\\" +
				"Classes\\Data system Arquitectures\\Project\\Data\\";
		String[] inputfiles = //{"movie_link.csv","keyword.csv","aka_name.csv"};//,"person_info.csv","cast_info.csv",
			{"movie_link_Ord.csv","keyword_Ord.csv","aka_name_Ord.csv"};	
		//};	
		
		int[] blocksize = {50000,100000,500000,1000000,5000000};
		int column_sort = 1;
		int[] numblock_merge = {10,25,50,100,200};
		int numsimulations = 5;
		
		long simulations = 0;
		
		File resultsFile =new File(folder + "results_ExtSort_Ord.csv");
	    BufferedWriter writer = new BufferedWriter(new FileWriter(resultsFile));
	    writer.write("file,simulation,blocksize,numblock_merge,time\n");
		
		for (String f:inputfiles){
			String filename = folder + f;
			for (int i = 0 ; i< numsimulations; i++){
				for(int j = 0;j<blocksize.length;j++){
					for (int k=0;k<numblock_merge.length;k++){
						Experiments_MergeSort exp = new Experiments_MergeSort(folder, f, blocksize[j], column_sort,numblock_merge[k]);
						simulations = exp.ExternalMergeSort();
						writer.write(f + "," + i + "," + blocksize[j] + "," + numblock_merge[k] + "," + simulations + '\n');
					}
				}
			}
		}
		
		writer.close();
	}
	
}
