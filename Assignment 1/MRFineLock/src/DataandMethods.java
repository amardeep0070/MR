import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class DataandMethods {
	public  String cvsSplitBy = ",";
	public  ArrayList<String> temp = new ArrayList<>();
	public  HashMap<String,Double> avgTempStation= new HashMap<String, Double>();
	//ConcurrentHashMap is used here to ensure that only one thread adds data to the map at one time
	public  Map<String,double[]> tempCountMap= new ConcurrentHashMap<String,double[]>();
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
				double[] valueList=tempCountMap.get(record[0]);
				if(valueList!=null){
					//if the station ID is already present.The thread which wants to update the record, needs a lock
					//only on that particular record of the map, not the whole map.
					synchronized (valueList) {
						double[] temp = tempCountMap.get(record[0]);
						temp[0]=temp[0]+Double.parseDouble(record[3]);
						temp[1]=temp[1]+1;
						tempCountMap.put(record[0], temp);
						//Uncomment for Scenario C
						//fibonacci(17);
					}
				}
				else{
					//if the station id is absent the thread takes a lock on the whole data structure 
					//and then after getting the lock checks again if any other thread in mean time has not added that
					//record to the map, if not it just adds the new record in the map, else it updates the value as explained above.
					synchronized (this) {
						valueList=tempCountMap.get(record[0]);
						if(valueList!=null){
							synchronized (valueList) {
								double[] temp = tempCountMap.get(record[0]);
								temp[0]=temp[0]+Double.parseDouble(record[3]);
								temp[1]=temp[1]+1;
								//Uncomment for Scenario C
								//fibonacci(17);
								tempCountMap.put(record[0], temp);
							}
							
						}
						else{
							//No need to add anymore lock here as tempCountMap is a concurrent HashMap
							double[] temp = new double[]{Double.parseDouble(record[3]),1};
							tempCountMap.put(record[0], temp);
							
						}
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
