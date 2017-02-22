package pageRank_assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalCountMapper extends Mapper<LongWritable, Text,Text,NullWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] temp = value.toString().split("s@!");
		if(!temp[2].equals("[]")){
			String listContent=temp[2].substring(1, temp[2].length()-1);
			//List<String> list = new ArrayList<String>(Arrays.asList(listContent.split(" , ")));
			String[] list = listContent.split(",");
			for(String s: list){
				s=stripSpaces(s.trim());
				context.write(new Text(s), NullWritable.get());
			}

		}
		temp[0]=stripSpaces(temp[0].trim());
		context.write(new Text(temp[0]), NullWritable.get());
	}
	public String stripSpaces(String s){
		while(s.startsWith(" ")){
			s=s.substring(1,s.length());
			}
		return s;
	}
}
