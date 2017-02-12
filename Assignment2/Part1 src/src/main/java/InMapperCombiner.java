import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//This uses inMapperCombiniing 
public  class InMapperCombiner 
extends Mapper<LongWritable, Text, Text, Text>{
	public HashMap<String,Double[]> inMapCombiner ;
	public void setup(Context context){
		//new Hashmap key-stationId, Value-[maxTempSum,maxCount,minTempSum,minCount]
		inMapCombiner = new HashMap<String, Double[]>();
	}
	@Override
	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException {


		String[] temporary = value.toString().split(",");
		//accumulate before emitting 
		//if present add the sum and increment the count based on the type "TMAX" or "TMIN"
		if(inMapCombiner.containsKey(temporary[0])){
			Double[] minMaxTemp= inMapCombiner.get(temporary[0]);
			if(temporary[2].trim().equals("TMAX")){
				minMaxTemp[0]= minMaxTemp[0]+ Double.parseDouble(temporary[3]);
				minMaxTemp[1]++;
				inMapCombiner.put(temporary[0], minMaxTemp);
			}
			if(temporary[2].trim().equals("TMIN")){
				minMaxTemp[2]= minMaxTemp[2]+ Double.parseDouble(temporary[3]);
				minMaxTemp[3]++;
				inMapCombiner.put(temporary[0], minMaxTemp);
			}
		}
		//if not present add to the map.
		else{
			Double[] minMaxTemp= new Double[4];
			if(temporary[2].equals("TMAX")){
				minMaxTemp[0]=Double.parseDouble(temporary[3]);
				minMaxTemp[1]=1d;
				minMaxTemp[2]=0d;
				minMaxTemp[3]=0d;
				inMapCombiner.put(temporary[0], minMaxTemp);
			}
			if(temporary[2].trim().equals("TMIN")){
				minMaxTemp[0]=0d;
				minMaxTemp[1]=0d;
				minMaxTemp[2]=Double.parseDouble(temporary[3]);
				minMaxTemp[3]=1d;
				inMapCombiner.put(temporary[0], minMaxTemp);
			}

		}

	}
	//emit the accumulated values.
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(String key : inMapCombiner.keySet()){
			Double[] temporary = inMapCombiner.get(key);
			context.write(new Text(key), new Text(temporary[0]+","+ temporary[1] +","+ temporary[2] +","+ temporary[3]));
		}

	}
}