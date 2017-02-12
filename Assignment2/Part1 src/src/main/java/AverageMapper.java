import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class AverageMapper 
//Map outputs Text(StationId) as the key and Text(Type,temperature)
//where type is either TMAX or TMIN and temprature is the corresponding 
//temperature.
extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException {
		String[] temporary = value.toString().split(",");
		if(temporary[2].trim().equals("TMAX") || temporary[2].trim().equals("TMIN")){
			context.write(new Text(temporary[0]), new Text(temporary[2]+","+ temporary[3]));
		}
	}
}