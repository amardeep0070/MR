import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Worker extends Thread{
 private DataandMethods data;
 private List<String> job;
 private Map<String,double[]> jobOutput = new HashMap<String,double[]>();
 Worker (DataandMethods data, String name,List<String> job)
 {
	//Thread t = new Thread(name);
    super (name); // Save thread's name
    this.data = data; 
    this.job=job;
    //this.jobOutput=jobOutput;
 }
 @Override
 public void run(){
	// System.out.println(Thread.currentThread().getName() +"is running");
	 this.jobOutput=data.countandSum(job);
 }
 public Map<String,double[]> returnOutput(){
	 return this.jobOutput;
 }
}
