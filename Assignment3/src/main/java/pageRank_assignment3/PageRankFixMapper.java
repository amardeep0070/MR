package pageRank_assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageRankFixMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static Long numberOfPages;
	public static Long delta;
	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		numberOfPages = Long.parseLong(conf.get("numberOfPages"));
		delta=Long.parseLong(conf.get("delta"));
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		double pageRank=0d;
		String temp[]= value.toString().split("s@!");
		temp[0]=stripSpaces(temp[0].trim());
		pageRank= (Double.parseDouble(temp[1])+0.85*delta/(numberOfPages*100000000));
		context.write(new Text(temp[0]), new Text(pageRank+""));
		
	}
	public String stripSpaces(String s){
		while(s.startsWith(" ")){
			s=s.substring(1,s.length());
			}
		return s;
	}
}
