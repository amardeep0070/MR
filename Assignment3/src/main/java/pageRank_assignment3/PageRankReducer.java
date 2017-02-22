package pageRank_assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import pageRank_assignment3.Driver.delta;

public class PageRankReducer extends Reducer<Text,Text,Text,Text> {
	public static Long numberOfPages;
	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		numberOfPages = Long.parseLong(conf.get("numberOfPages"));
	}
	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context
			) throws IOException, InterruptedException {
		double pageRank=0d;
		String list="[]";
		for(Text val: values){
			if(val.toString().startsWith("list=")){
				list=val.toString().substring(5, val.toString().length());
				continue;
			}
			pageRank=pageRank+Double.parseDouble(val.toString());
		}
		pageRank=((0.15/numberOfPages)+(0.85*(pageRank)));
		if(list.equals("[]")) {
			Long convertedPageRank=Math.round(pageRank*100000000);
			context.getCounter(delta.DELTA).increment(convertedPageRank);
			pageRank=0d;
		}
		context.write(key, new Text("s@!"+pageRank+"s@!"+list));
	}
}
