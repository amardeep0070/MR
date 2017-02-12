import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//outputs staionId as the key and Text(mean Minimum Temp, mean Max temp).
public  class AvgReducer 
	extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			double maxTempSum = 0;
			double minTempSum = 0;
			Double maxCount= 0d;
			Double minCount=0d;
			Double maxAvg=null;
			Double minAvg=null;
			for (Text val : values) {
				String[] temporary= val.toString().split(",");
				//used when noCombiner or inMapper combining is used.
				if(temporary.length==2){
					//check if the type is TMIN or TMAX
					if(temporary[0].trim().equals("TMAX") && !temporary[1].equals("null")){
						maxTempSum=maxTempSum+ Double.parseDouble(temporary[1]);
						maxCount++;
					}
					if(temporary[0].trim().equals("TMIN") && !temporary[1].equals("null")){
						minTempSum=minTempSum+ Double.parseDouble(temporary[1]);
						minCount++;
					}
				}
				//Used when the combiner is called or this is used with the inMapper Combining.
				else{
					maxTempSum=maxTempSum+Double.parseDouble(temporary[0]);
					maxCount=maxCount+Double.parseDouble(temporary[1]);
					minTempSum=minTempSum+Double.parseDouble(temporary[2]);
					minCount=minCount+Double.parseDouble(temporary[3]);
				}
				
			}
			if(maxCount>0){
				 maxAvg=maxTempSum/maxCount;
			}
			if(minCount>0){
				 minAvg=minTempSum/minCount;
			}
			//emit
			 context.write(key, new Text("Mean Minimum Temp is " + minAvg + "\n" + 
					 "Mean Maximum Temp is " +maxAvg));
		}
	}