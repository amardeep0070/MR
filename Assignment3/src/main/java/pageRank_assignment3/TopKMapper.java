package pageRank_assignment3;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopKMapper extends Mapper<LongWritable, Text, Text, Text> {
	public TreeMap<Double,String> top100;
	@Override
	public void setup(Context context){
		top100 = new TreeMap<>();
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String temp[]= value.toString().split("\t");
		temp[0]=stripSpaces(temp[0].trim());
		temp[1]=stripSpaces(temp[1].trim());
		if(top100.size()>100){
			top100.pollFirstEntry();
		}
		top100.put(Double.parseDouble(temp[1]),temp[0]);
	}
	public String stripSpaces(String s){
		while(s.startsWith(" ")){
			s=s.substring(1,s.length());
		}
		return s;
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Set set =top100.descendingKeySet();
		Iterator iterator = set.iterator();
		while(iterator.hasNext()) {
			 Map.Entry mentry = (Map.Entry)iterator.next();
			context.write(new Text(mentry.getKey().toString()), new Text(mentry.getValue().toString()));
		}
	}
}
