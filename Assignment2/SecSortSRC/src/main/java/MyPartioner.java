import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//used for Partitioning  to the particular reducer
public class MyPartioner extends Partitioner<MyKey,Text>{

	@Override
	public int getPartition(MyKey key, Text value, int numPartitions) {
		key.getStationId();
		//Partitioning  only on the basis of StaionID and ignoring the year.
		//This ensures that all the records for a particular StationId are sent to to a single Reducer.
		return Math.abs(key.getStationId().hashCode()%numPartitions);
	}

}
