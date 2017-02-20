package pageRank_assignment3;

import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	public static enum pageCount{
		COUNT
	};
	public static void main(String[] args) throws Exception {
		PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
		String outPutCount="outputCount";
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "SecSortDriver");
		job.setJarByClass(Driver.class);
		job.setMapperClass(GraphMakerMapper.class);
		//job.setGroupingComparatorClass(MyGroupComparator.class);
		job.setReducerClass(GraphMakerReducer.class);
		//job.setPartitionerClass(MyPartioner.class);
		//Set MapOutput to MyKey.class
		job.setMapOutputKeyClass(Text.class);
		//job.setOutputKeyClass(LongWritable.class);
		//job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		if (success) {
			Configuration totalCountConfig = new Configuration();
			Job countTotal=Job.getInstance(totalCountConfig,"counttotal");
			countTotal.setJarByClass(Driver.class);
			countTotal.setMapperClass(TotalCountMapper.class);
			countTotal.setReducerClass(TotalCountRedcuer.class);
			countTotal.setMapOutputValueClass(NullWritable.class);
			countTotal.setMapOutputKeyClass(Text.class);
			FileInputFormat.addInputPath(countTotal, new Path(otherArgs[otherArgs.length-1]));
			FileOutputFormat.setOutputPath(countTotal, new Path(outPutCount));
			if(countTotal.waitForCompletion(true)){
				writer.println(countTotal.getCounters().findCounter(pageCount.COUNT).getValue());
				writer.close();
				System.exit(0);
			}
			//System.exit(countTotal.waitForCompletion(true) ? 0 : 1);
		}
		
	}
}
