package pageRank_assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopKMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String temp[]= value.toString().split("s@!");
		temp[0]=stripSpaces(temp[0].trim());
		context.write(new Text(temp[1]), new Text(temp[0]));
	}
	public String stripSpaces(String s){
		while(s.startsWith(" ")){
			s=s.substring(1,s.length());
			}
		return s;
	}
}
