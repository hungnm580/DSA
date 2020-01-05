package dsa.experiment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;

import dsa.read.MMapStreaming_Read;
import dsa.read.StreamReader;
import dsa.write.LineStreaming_Write;
import dsa.write.MMapStreaming_Write;
import dsa.write.StreamWriter;

public class Experiments_MergeSort {

	private String filename;
	private String folder;
	private int blockSize; // BlockSize
	private int columnNumber; //ColumnSorted
	private int num_blocksmerge; //Streams to merge
	private int num_streams; // NumberOfStreams
	private Queue<String> nameFiles;
	private String tempfolder;
	private PriorityQueue<String> queueLines;
	private PriorityQueue<FileChunk> queueTourtnament;
	private int chunkNumber;
	
	public Experiments_MergeSort(String p, String f, int M, int k, int d){
		filename = f;
		folder = p;
		blockSize = M;
		columnNumber = k;
		num_blocksmerge = d;
	}
	
	public void writeChunkFile(int chunk, int sizeChunk){
		
		StreamWriter writer = new MMapStreaming_Write(tempfolder + filename.replace(".csv","_"+chunk+".csv"),10000000,sizeChunk);
		try {
			writer.stream_openFile();
		
			while(!queueLines.isEmpty()){
				String line_write = queueLines.poll();
				writer.stream_writeLine(line_write); 
			}
			writer.stream_close();
		} catch (Exception e){
			e.printStackTrace();
			
		}
			
		nameFiles.add(filename.replace(".csv","_"+chunk+".csv"));
		
	}
	
	public void phase1_generateSortedSubFiles(){
		
		tempfolder = folder+"Temp\\";
		try{
			File file = new File(tempfolder);
			file.mkdir();
		}
		catch(Exception e){	
			e.printStackTrace();
		}
		
		chunkNumber = 0;
		
		StreamReader input = new MMapStreaming_Read(folder + filename, 10000000);
		
		queueLines = new PriorityQueue<String>(new Comparator<String>(){
			public int compare(String line1 , String line2){
				String[] columnsline1 = line1.split(",");
				String[] columnsline2 = line2.split(",");
				return columnsline1[columnNumber].trim().compareTo(columnsline2[columnNumber].trim());
			}
		}); 
		
		nameFiles = new LinkedList<String>();
		
		try {
			input.stream_openFile();
			int sizeChunk = 0;
			while(true){
				String line = input.stream_readLine();
				if (input.stream_eof()){
					writeChunkFile(chunkNumber,sizeChunk);
					chunkNumber ++;
					break;
				}
				if (sizeChunk + line.length() < blockSize){
					sizeChunk += line.length();
					queueLines.add(line);
				}
				else{
					writeChunkFile(chunkNumber,sizeChunk);
					sizeChunk = line.length();
					queueLines.add(line);
					chunkNumber ++;
				}
			}
			input.stream_close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	class FileChunk{
		public int chunk;
		public String line;
		
		public FileChunk(int c, String l){
			chunk = c;
			line = l;
		}
	}
	
	public void phase2_mergechunks(){
		
		String outputfolder = folder+"Out\\";
		
		try{
			File file = new File(outputfolder);
			file.mkdir();
		}
		catch(Exception e){	
		}
		
		queueTourtnament = new PriorityQueue<FileChunk>(new Comparator<FileChunk>(){
			public int compare(FileChunk line1 , FileChunk line2){
				String[] columnsline1 = line1.line.split(",");
				String[] columnsline2 = line2.line.split(",");
				return columnsline1[columnNumber].trim().compareTo(columnsline2[columnNumber].trim());
			}
		});
		
		try{
				
			while(nameFiles.size()>1){
				
				String fileOutputPath;
				if(nameFiles.size() <= num_blocksmerge){
					fileOutputPath = outputfolder + filename.replace(".csv","_"+chunkNumber+".csv");
				}
				else{
					fileOutputPath = tempfolder + filename.replace(".csv","_"+chunkNumber+".csv");
				}
				
				//Number of merge blocks needed (max or required)
				num_blocksmerge = Math.min(num_blocksmerge,nameFiles.size());
				// readers of every block possible to merge
				StreamReader[] mergeblocks_chunks = new MMapStreaming_Read[num_blocksmerge];
				
				int outputfilesize = 0;
				
				for (int i=0;i<num_blocksmerge;i++){
					String chunkFile_name = nameFiles.poll();
					mergeblocks_chunks[i] = new MMapStreaming_Read(tempfolder + chunkFile_name, 10000000); 
					mergeblocks_chunks[i].stream_openFile();
					
					File f = new File(tempfolder + chunkFile_name);
					outputfilesize += f.length();
					
					//First Call
					String line = mergeblocks_chunks[i].stream_readLine();
					FileChunk fc = new FileChunk(i,line);
					queueTourtnament.add(fc);
				}
				
				StreamWriter writer = new MMapStreaming_Write(fileOutputPath,10000000,outputfilesize);
				writer.stream_openFile();
				
				while(!queueTourtnament.isEmpty()){
					//Choose winner (first in order) of the d chunks
					FileChunk firstLine = queueTourtnament.poll();
					//Write winner line from the ordered chunk files
					writer.stream_writeLine(firstLine.line);
					//Read new Line from the selected previous chunk
					String line = mergeblocks_chunks[firstLine.chunk].stream_readLine();
					// Validate if file has more lines
					if (!mergeblocks_chunks[firstLine.chunk].stream_eof()){
						FileChunk fc = new FileChunk(firstLine.chunk,line);
						// Add new line with id chunk to the queue
						queueTourtnament.add(fc);
					}
				}
				
				for (int i=0;i<num_blocksmerge;i++){
					mergeblocks_chunks[i].stream_close();
				}
				
				writer.stream_close();
				nameFiles.add(filename.replace(".csv","_"+chunkNumber+".csv"));
				chunkNumber ++;
			}
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void cleaning_phases(){
		try{
			System.gc();
			Thread.sleep(100);
			System.gc();
			File dir = new File(tempfolder);
			for (File file : dir.listFiles()) {
			    FileDeleteStrategy.FORCE.delete(file);
			}  
			//FileUtils.forceDelete(new File(tempfolder));
			Thread.sleep(100);
		}
		catch(Exception e){	
			e.printStackTrace();
		}
	}
	
	public long ExternalMergeSort(){
		long startTime = System.currentTimeMillis();
		System.out.println("File - "+ filename);
		
		phase1_generateSortedSubFiles();
		phase2_mergechunks();		
		
		long endTime = System.currentTimeMillis();
		
		cleaning_phases();
		
		long timeElapsed = endTime - startTime;
		System.out.printf("Execution time in milliseconds: %d \n" , timeElapsed);
		
		return timeElapsed;
	}
	
}
