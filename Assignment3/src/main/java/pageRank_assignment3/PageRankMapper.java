package pageRank_assignment3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pageRank_assignment3.Driver.delta;


public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
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
		context.write(new Text(temp[0]), new Text("list="+temp[2]));
		if(!temp[1].equals("PRV")){
			pageRank= (Double.parseDouble(temp[1])+0.85*delta/(numberOfPages*100000000));
		}
		if(temp[1].equals("PRV")){
			pageRank=1d/numberOfPages;
		}
		if(!temp[2].equals("[]")){
			String preList=temp[2].substring(1, temp[2].length()-1);
			String[] list = preList.split(",");
			for(String l : list){
				l=stripSpaces(l.trim());
				context.write(new Text(l),new Text(""+pageRank/temp[2].length()));
			}
			pageRank=0d;
		}
		
			//context.write(new Text(temp[0].trim()),new Text(""+pageRank));
	}
	public String stripSpaces(String s){
		while(s.startsWith(" ")){
			s=s.substring(1,s.length());
			}
		return s;
	}
}
