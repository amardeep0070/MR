import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class SortMapper 
//Map outputs a "MyKey" as the key and Text(Type,temperature)
//where type is either TMAX or TMIN and temprature is the corresponding 
//temperature.
extends Mapper<LongWritable, Text, MyKey, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException {
		String[] temporary = value.toString().split(",");
		String year= temporary[1].trim().substring(0, 4);
		if(temporary[2].trim().equals("TMAX") || temporary[2].trim().equals("TMIN")){
			MyKey tupple = new MyKey(temporary[0],year);
			context.write(tupple, new Text(temporary[2]+ ","+ temporary[3]));
		}
	}
}