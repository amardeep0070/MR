import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//Combiner Class
public  class AvgCombiner 
	extends Reducer<Text,Text,Text,Text> {

	//Takes in the Text as the Key and Text as the value.
	//Key- stationId, value-(maxTempSum,maxCount,minTempSum,minCount).
	//Reducer handles both cases that use the combiner and those that dont too.
	// Combines the data in the map call before emitting,
	//thereby reducing the data sent.
		@Override
		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			double maxTempSum = 0;
			double minTempSum = 0;
			Double maxCount= 0d;
			Double minCount=0d;
			for (Text val : values) {
				String[] temporary= val.toString().split(",");
				if(temporary[0].trim().equals("TMAX")){
					maxTempSum=maxTempSum+ Double.parseDouble(temporary[1]);
					maxCount++;
				}
				if(temporary[0].trim().equals("TMIN")){
					minTempSum=minTempSum+ Double.parseDouble(temporary[1]);
					minCount++;
				}
			}
			//
			context.write(key, new Text(maxTempSum+","+ maxCount+","+ minTempSum+","+ minCount));
		}
	}