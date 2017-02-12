import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Recieves MyKey as the Key and Text as the value.
//For eg-
//A[(1880,TMAX,10),(1880,TMIN,-10)...(1889,TMIN,1)]
//the years here will be extracted from the key and not a part of value just shown here for reading purpose.
//with the help of the groupPationer I ensure that all the records for a particular stationID for all the years
//in sorted order, end up in one reduce call.So I itrate over the list of values and calculate average and emit the 
//mean Max temp and Mean avg temp once record for a new year comes in cause I know once the year changes there wont be anymore 
//records of that particular year.
public  class SortReducer 
	extends Reducer<MyKey,Text,Text,Text> {

		@Override
		public void reduce(MyKey key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			String temperatures="[";
			int i=0;
			double maxTempSum = 0;
			double minTempSum = 0;
			Double maxCount= 0d;
			Double minCount=0d;
			Double maxAvg=null;
			Double minAvg=null;
			String currentYear="null";
			//itrate over the values [(1880,TMIN,-10),...]
			for (Text val : values) {
				String[] temporary= val.toString().split(",");
				//Check if the year changes then accumulate in a String which will be written after this reduce call.
				if((!currentYear.equals(key.getYear()) && !currentYear.equals("null"))){
					if(maxCount>0){
						 maxAvg=maxTempSum/maxCount;
					}
					if(minCount>0){
						 minAvg=minTempSum/minCount;
					}
					String tempYear="("+ currentYear + ","+ minAvg+","+maxAvg+")";
					if(temperatures.length()>1){
						temperatures=temperatures+","+tempYear;
					}
					else{
						temperatures=temperatures+tempYear;
					}
					//reset the Sum and count after the year changes.
					 maxTempSum = 0;
					 minTempSum = 0;
					 maxCount= 0d;
					 minCount=0d;
					 maxAvg=null;
					 minAvg=null;
				}
				currentYear=key.getYear();
				if(temporary.length==2){
					if(temporary[0].trim().equals("TMAX") && !temporary[1].equals("null")){
						maxTempSum=maxTempSum+ Double.parseDouble(temporary[1]);
						maxCount++;
					}
					if(temporary[0].trim().equals("TMIN") && !temporary[1].equals("null")){
						minTempSum=minTempSum+ Double.parseDouble(temporary[1]);
						minCount++;
					}
				}
//				else{
//					maxTempSum=maxTempSum+Double.parseDouble(temporary[0]);
//					maxCount=maxCount+Double.parseDouble(temporary[1]);
//					minTempSum=minTempSum+Double.parseDouble(temporary[2]);
//					minCount=minCount+Double.parseDouble(temporary[3]);
//				}
				
				
			}
			if(maxCount>0){
				 maxAvg=maxTempSum/maxCount;
			}
			if(minCount>0){
				 minAvg=minTempSum/minCount;
			}
			//Emitting the StaionId and temperatures for each year.
			//tempYear contains the data for the last year for the list.
			String tempYear="("+ currentYear + ","+ minAvg+","+maxAvg+")";
			 context.write(new Text(key.getStationId()+","), new Text(temperatures+tempYear+"]"));
		}
	}