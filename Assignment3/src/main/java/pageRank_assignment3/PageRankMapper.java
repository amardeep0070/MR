package pageRank_assignment3;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static Long numberOfPages;
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		numberOfPages = Long.parseLong(conf.get("property.name"));
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Long pageRank=0l;
		String temp[]= value.toString().split("s@!");
		context.write(new Text(temp[0]), new Text(temp[2]));
		if(!temp[1].equals("PRV")){
			pageRank=Long.parseLong(temp[1]);
		}
		if(temp[1].equals("PRV")){
			pageRank=1/numberOfPages;
		}
		if(!temp[2].equals("[]")){
			String[] list = temp[2].split(",");
			for(String l : list){
				context.write(new Text(temp[0]),new Text(""+pageRank));
			}
		}
	}
}
