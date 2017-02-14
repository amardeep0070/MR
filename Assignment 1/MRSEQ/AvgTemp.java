import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class AvgTemp {
	public String cvsSplitBy = ",";
	//Structure which contains the final station Ids and their averages.
	public static HashMap<String,Double> avgTempStation= new HashMap<String, Double>();
	public static void main(String[] args) {
		//Structure which contains data read from the file.
		ArrayList<String> datafromFile = new ArrayList<String>();
		String csvFile = "/Users/Amardeep/Downloads/1912.csv";
		datafromFile=readFiletoList(csvFile);
		long minTime=Integer.MAX_VALUE;
		long maxTime=Integer.MIN_VALUE;
		long totalTime=0;

		//Running the code 10 times to find the average,min and max running time.
		for(int i=0; i<10; i++){
			long startTime = System.currentTimeMillis();
			AvgTemp a = new AvgTemp();
			avgTempStation=a.avgTemp(a.seq(datafromFile));
			long timeTaken=System.currentTimeMillis()-startTime;
			totalTime=totalTime+timeTaken;
			minTime=Math.min(minTime, timeTaken);
			maxTime=Math.max(maxTime, timeTaken);
		}

		//Uncomment the next line to see the actual average per station
		//System.out.println(avgTempStation.toString());
		System.out.println("Minimum time is ="+ minTime);
		System.out.println("Maximum time is ="+ maxTime);
		System.out.println("Average time is ="+ totalTime/10);
	}
	
	//This reads the file and returns a list.
	public static ArrayList readFiletoList(String filePath){
		ArrayList<String> temp = new ArrayList<>();
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
	
	//This is where the records are being filterd by "TMAX" and grouped by station IDs.
	//returns a hashmap with key as the station ID and value as an array containing the final sum and the count of the total times
	//a station is seen.
	public  HashMap<String, double[]> seq(ArrayList<String> data){
		//key-StaionID
		//value-> 0-totatemp(running sum) ->1-count
		HashMap<String,double[]> tempCountMap= new HashMap<String,double[]>();
		for(String s : data){
			String[] record = s.split(cvsSplitBy);
			//filter out the records which are not of type "TMAX"
			if(record[2].equals("TMAX")){
				//if the stationID is present add the temp to the total and increment the count.
				if(tempCountMap.containsKey(record[0])){
					double[] temp = tempCountMap.get(record[0]);
					temp[0]=temp[0]+Double.parseDouble(record[3]);
					temp[1]=temp[1]+1;
					fibonacci(17);
					tempCountMap.put(record[0], temp);
				}
				//if not present add a new entry to the Map 
				else{
					double[] temp = new double[]{Double.parseDouble(record[3]),1};
					tempCountMap.put(record[0], temp);
				}
				
			}
			else continue;
		}
		
		return tempCountMap;
	}
	
	//This calculated the average per station ID.
	public HashMap<String, Double> avgTemp(HashMap<String,double[]> tempCountMap){
		HashMap<String,Double> avgTemp= new HashMap<String, Double>();
		for(String s: tempCountMap.keySet()){
			double[] temp = tempCountMap.get(s);
			double avgofStation= temp[0]/temp[1];
			avgTemp.put(s, avgofStation);
		}
		return avgTemp;
	}
	public void fibonacci(int n){
		 int febCount = n;
       int[] feb = new int[febCount];
       feb[0] = 0;
       feb[1] = 1;
       for(int i=2; i < febCount; i++){
           feb[i] = feb[i-1] + feb[i-2];
       }
       // for(int i=0; i<febCount; i++){
    	  
       // }


	}
}


