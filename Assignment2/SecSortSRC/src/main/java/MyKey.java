import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//CompositeKey "MyKey" is used as output from the Map and input to the Reduce. 
public class MyKey implements WritableComparable<MyKey>{
	//Key contains a tupple of StationId and Year.
	private String stationId;
	private String year;
	
	public MyKey(){
		//super();
	}
	public MyKey(String staionId, String year){
		this.stationId=staionId;
		this.year=year;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stationId);
		out.writeUTF(year);
	}

	public void readFields(DataInput in) throws IOException {
		this.stationId=in.readUTF();
		this.year=in.readUTF();
	}

	//compare both stationID and year.
	public int compareTo(MyKey o) {
		int objectComapre=this.stationId.compareTo(o.getStationId());
		if(objectComapre!=0){
			return objectComapre;
		}
		return this.year.compareTo(o.year);
	}
	//getters and Setters
	public String getStationId() {
		return stationId;
	}
	public void setStationId(String stationId) {
		this.stationId = stationId;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	
}

