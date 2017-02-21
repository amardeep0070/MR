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
				context.write(new Text(s), NullWritable.get());
			}

		}
		context.write(new Text(temp[0]), NullWritable.get());
	}
}
