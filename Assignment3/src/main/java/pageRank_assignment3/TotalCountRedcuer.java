package pageRank_assignment3;



import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pageRank_assignment3.Driver.pageCount;
public class TotalCountRedcuer extends Reducer<Text,Text,Text,Text>  {
	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context
			) throws IOException, InterruptedException {
		context.getCounter(pageCount.COUNT).increment(1);
		context.write(key, new Text(""));
	}
}