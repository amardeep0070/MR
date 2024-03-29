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
	public static enum delta{
		DELTA
	}
	public static void main(String[] args) throws Exception {
		Long numberOfPages=0l;
		Long dangling=0l;
		PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
		String outPutCount="outputCount";
//		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		if (otherArgs.length < 2) {
//			System.err.println("Usage: wordcount <in> [<in>...] <out>");
//			System.exit(2);
//		}
//		Job job = Job.getInstance(conf, "SecSortDriver");
//		job.setJarByClass(Driver.class);
//		job.setMapperClass(GraphMakerMapper.class);
//		//job.setGroupingComparatorClass(MyGroupComparator.class);
//		job.setReducerClass(GraphMakerReducer.class);
//		//job.setPartitionerClass(MyPartioner.class);
//		//Set MapOutput to MyKey.class
//		job.setMapOutputKeyClass(Text.class);
//		//job.setOutputKeyClass(LongWritable.class);
//		//job.setOutputValueClass(Text.class);
//		for (int i = 0; i < otherArgs.length - 1; ++i) {
//			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//		}
//		FileOutputFormat.setOutputPath(job,
//				new Path(otherArgs[otherArgs.length - 1]+"0"));
//		//System.exit(job.waitForCompletion(true) ? 0 : 1);
//		boolean success = job.waitForCompletion(true);
//		if (success) {
//			int iteration=0;
//			Configuration totalCountConfig = new Configuration();
//			Job countTotal=Job.getInstance(totalCountConfig,"counttotal");
//			countTotal.setJarByClass(Driver.class);
//			countTotal.setMapperClass(TotalCountMapper.class);
//			countTotal.setReducerClass(TotalCountRedcuer.class);
//			countTotal.setMapOutputValueClass(NullWritable.class);
//			countTotal.setMapOutputKeyClass(Text.class);
//			FileInputFormat.addInputPath(countTotal, new Path(otherArgs[otherArgs.length-1]+"0"));
//			FileOutputFormat.setOutputPath(countTotal, new Path(outPutCount));
//			if(countTotal.waitForCompletion(true)){
//				numberOfPages=countTotal.getCounters().findCounter(pageCount.COUNT).getValue();
//				//writer.close();
//				//System.exit(0);
//			}
//			 
//			while(iteration<10){
//				String inputPath=otherArgs[otherArgs.length-1]+iteration;
//				iteration++;
//				String outputPath=otherArgs[otherArgs.length-1]+iteration;
//				Configuration pageRankConfig = new Configuration();
//				Job pageRank = Job.getInstance(pageRankConfig,"pageRank");
//				pageRank.getConfiguration().setLong("delta", dangling);
//				pageRank.setJarByClass(Driver.class);
//				pageRank.setMapperClass(PageRankMapper.class);
//				pageRank.setReducerClass(PageRankReducer.class);
//				pageRank.setMapOutputKeyClass(Text.class);
//				FileInputFormat.addInputPath(pageRank, new Path(inputPath));
//				FileOutputFormat.setOutputPath(pageRank, new Path(outputPath));
//				pageRank.getConfiguration().setLong("numberOfPages", numberOfPages);
//				if(pageRank.waitForCompletion(true)){
//					dangling=pageRank.getCounters().findCounter(delta.DELTA).getValue();
//				}
//		
//			}
//			String finalInput=otherArgs[otherArgs.length-1]+"10";
//			String finalOutput=otherArgs[otherArgs.length-1]+"Final";
//			Configuration pageRankFixConfig = new Configuration();
//			Job pageRankFix = Job.getInstance(pageRankFixConfig,"pageRank");
//			pageRankFix.setMapperClass(PageRankFixMapper.class);
//			pageRankFix.setMapOutputKeyClass(Text.class);
//			pageRankFix.getConfiguration().setLong("delta", dangling);
//			pageRankFix.getConfiguration().setLong("numberOfPages", numberOfPages);
//			FileInputFormat.addInputPath(pageRankFix, new Path(finalInput));
//			FileOutputFormat.setOutputPath(pageRankFix, new Path(finalOutput));
//			pageRankFix.waitForCompletion(true);
//		}
		if (true) {
			int iteration=0;
//			Configuration totalCountConfig = new Configuration();
//			Job countTotal=Job.getInstance(totalCountConfig,"counttotal");
//			countTotal.setJarByClass(Driver.class);
//			countTotal.setMapperClass(TotalCountMapper.class);
//			countTotal.setReducerClass(TotalCountRedcuer.class);
//			countTotal.setMapOutputValueClass(NullWritable.class);
//			countTotal.setMapOutputKeyClass(Text.class);
//			FileInputFormat.addInputPath(countTotal, new Path("preprocessing"+"0"));
//			FileOutputFormat.setOutputPath(countTotal, new Path(outPutCount));
//			if(countTotal.waitForCompletion(true)){
//				numberOfPages=countTotal.getCounters().findCounter(pageCount.COUNT).getValue();
//				//writer.close();
//				//System.exit(0);
//			}
			 
			while(iteration<4){
				String inputPath="preprocessing"+iteration;
				iteration++;
				String outputPath="preprocessing"+iteration;
				Configuration pageRankConfig = new Configuration();
				Job pageRank = Job.getInstance(pageRankConfig,"pageRank");
				pageRank.getConfiguration().setLong("delta", dangling);
				pageRank.setJarByClass(Driver.class);
				pageRank.setMapperClass(PageRankMapper.class);
				pageRank.setReducerClass(PageRankReducer.class);
				pageRank.setMapOutputKeyClass(Text.class);
				FileInputFormat.addInputPath(pageRank, new Path(inputPath));
				FileOutputFormat.setOutputPath(pageRank, new Path(outputPath));
				pageRank.getConfiguration().setLong("numberOfPages", 4);
				if(pageRank.waitForCompletion(true)){
					dangling=pageRank.getCounters().findCounter(delta.DELTA).getValue();
				}
			}
			String finalInput="preprocessing"+"4";
			String finalOutput="preprocessing"+"Final";
			Configuration pageRankFixConfig = new Configuration();
			Job pageRankFix = Job.getInstance(pageRankFixConfig,"pageRank");
			pageRankFix.setMapperClass(PageRankFixMapper.class);
			pageRankFix.setMapOutputKeyClass(Text.class);
			pageRankFix.getConfiguration().setLong("delta", dangling);
			pageRankFix.getConfiguration().setLong("numberOfPages", 4);
			FileInputFormat.addInputPath(pageRankFix, new Path(finalInput));
			FileOutputFormat.setOutputPath(pageRankFix, new Path(finalOutput));
			pageRankFix.waitForCompletion(true);
		}
		
	}
}
