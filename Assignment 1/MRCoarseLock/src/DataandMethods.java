import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class DataandMethods {
	public  String cvsSplitBy = ",";
	public  ArrayList<String> temp = new ArrayList<>();
	public  HashMap<String,Double> avgTempStation= new HashMap<String, Double>();
	//This is the HashMap which is shared and updated by all the workers and contains the station ID as the key and 
	//running sum and count as the value.
	public  Map<String,double[]> tempCountMap= new HashMap<String,double[]>();
	//This is the final output containing the Station ID(Key) and their avg temp (Value).
	public  HashMap<String,Double> avgTemp= new HashMap<String, Double>();
	public  List<String> workerJob= new ArrayList<String>();
	public void countandSum(List<String> data){

		//key-StaionID
		//value- 0-totaltemp 1-count
		for(String s : data){
			String[] record = s.split(cvsSplitBy);
			//filter out the records which are not of type "TMAX"
			if(record[2].equals("TMAX")){
				//if the stationID is present add the temp to the total and increment the count.
				//this part of the code is synchronized and access to the whole data structure tempCountMap is assured to be thread safe
				//as this is the only part of code modifying this shared structure.
				synchronized (this) {
					if(tempCountMap.containsKey(record[0])){
						double[] temp = tempCountMap.get(record[0]);
						temp[0]=temp[0]+Double.parseDouble(record[3]);
						temp[1]=temp[1]+1;
						//Uncomment for Scenario C
						//fibonacci(17);
						tempCountMap.put(record[0], temp);
					}
					//if not present add a new entry to the Map 
					else{
						double[] temp = new double[]{Double.parseDouble(record[3]),1};
						tempCountMap.put(record[0], temp);
					}	
					
				}
				
			}
			else continue;
		}
	}
	public  void avgTemp(Map<String,double[]> tempCountMap){
		for(String s: tempCountMap.keySet()){
			double[] temp = tempCountMap.get(s);
			double avgofStation= temp[0]/temp[1];
			avgTemp.put(s, avgofStation);
		}
	}
	public void fibonacci(int n){
		 int febCount = n;
       int[] feb = new int[febCount];
       feb[0] = 0;
       feb[1] = 1;
       for(int i=2; i < febCount; i++){
           feb[i] = feb[i-1] + feb[i-2];
       }

//       for(int i=0; i< febCount; i++){
//              // System.out.print(feb[i] + " ");
//       }
	}
}
