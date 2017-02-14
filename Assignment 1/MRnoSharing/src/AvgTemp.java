import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class AvgTemp {
	public static String csvFile = "/Users/Amardeep/Downloads/1912.csv";
	public static  int numberofThreads=4;
	//Runtime.getRuntime().availableProcessors();
	public static  int startIndex=0;
	public static  int incrementSplit=0;
	public static  int endIndex=0;
	public static  int sizeofData=0;
	public static ArrayList readFiletoList(String filePath){
		ArrayList<String> temp = new ArrayList<String>();
		BufferedReader br = null;
		String line = "";
		try {

			br = new BufferedReader(new FileReader(filePath));
			while ((line = br.readLine()) != null) {
				temp.add(line);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return temp;
	}
	public static void main(String[] args) throws InterruptedException { 
		ArrayList<String> temp=readFiletoList(csvFile);
		sizeofData=temp.size();
		//System.out.println(sizeofData);
		incrementSplit=(int) Math.ceil(sizeofData/numberofThreads);
		
		//creating jobs for the workers by spliting ~equaly among all threads.
		endIndex=startIndex+incrementSplit;
		List<String> job1= temp.subList(startIndex, endIndex);
		startIndex=endIndex;
		endIndex=startIndex+incrementSplit;
		List<String> job2= temp.subList(startIndex, endIndex);
		startIndex=endIndex;
		endIndex=startIndex+incrementSplit;
		List<String> job3= temp.subList(startIndex, endIndex);
		startIndex=endIndex;
		endIndex=sizeofData;
		List<String> job4= temp.subList(startIndex, endIndex);
		
		
		//Code for running the code 10 times for finding min, max and average running times.
		long minTime=Integer.MAX_VALUE;
		long maxTime=Integer.MIN_VALUE;
		long totalTime=0;
		for(int j=0; j<10; j++){
			long startTime = System.currentTimeMillis();
			//spawning the threads
			
			DataandMethods data = new DataandMethods();
			Worker w1 = new Worker(data,"Worker1",job1);
			Worker w2 = new Worker(data,"Worker2",job2);
			Worker w3 = new Worker(data,"Worker3",job3);
			Worker w4 = new Worker(data,"Worker4",job4);
			List<Worker> workers = new ArrayList<>();
			workers.add(w1);
			workers.add(w2);
			workers.add(w3);
			workers.add(w4);
			for(Worker w : workers){
				w.start();
			}
			for(Worker w : workers){
				w.join();
			}
			//Every thread here has it own datastructure which once all threads are done need to be merged into one big hashmap
			//returnOuput returns the hashmap which is made after each thread finishes its processing.
			Map<String,double[]> tempCountMap= w1.returnOutput();
			for(int i=1;i<workers.size();i++){
				Map<String,double[]> tempMap = workers.get(i).returnOutput();
				for(String s : tempMap.keySet()){
					if(tempCountMap.containsKey(s)){
						double[] tempArray = tempCountMap.get(s);
						tempArray[0]=tempArray[0]+tempMap.get(s)[0];
						tempArray[1]=tempArray[1]+tempMap.get(s)[1];
						tempCountMap.put(s, tempArray);
					}
					else{
						tempCountMap.put(s, tempMap.get(s));
					}
				}
			}
			data.avgTemp(tempCountMap);
			long timeTaken=System.currentTimeMillis()-startTime;
			//System.out.println(timeTaken);
			totalTime=totalTime+timeTaken;
			minTime=Math.min(minTime, timeTaken);
			maxTime=Math.max(maxTime, timeTaken);
		}
		System.out.println("Minimum time is ="+ minTime);
		System.out.println("Maximum time is ="+ maxTime);
		System.out.println("Average time is ="+ totalTime/10);
		
		
	}

}
