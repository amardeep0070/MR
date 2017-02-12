
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class MyGroupComparator extends WritableComparator {

		public MyGroupComparator() {
			super(MyKey.class, true);
		}

		//This ensures that a single reduce call gets all the records for a particular staionId and also the list 
		//we get will be sorted by year. assuming the staionID has an entry for all the years we will get 
		//something like:-
		//the year will be extracted from the key and is not the part of the value.
		//(A,[(1880,TMAX,20),(1800,TMIN,-9)....(1889,TMAX,10)..]
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		MyKey key1 = (MyKey) o1;
		MyKey key2 = (MyKey) o2;
		return key1.getStationId().compareTo(key2.getStationId());
	}
}