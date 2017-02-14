import java.util.List;


public class Worker extends Thread{
 private DataandMethods data;
 private List<String> job;
 Worker (DataandMethods data, String name,List<String> job)
 {
    super (name); // Save thread's name
    this.data = data; 
    this.job=job;
 }
 @Override
 public void run(){
	 data.countandSum(job);
 }
}
