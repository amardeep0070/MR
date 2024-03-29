package pageRank_assignment3;



import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphMakerReducer extends Reducer<Text,Text,Text,Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context
			) throws IOException, InterruptedException {
		for (Text val : values) {
			context.write(key, val);
		}
	}
}
