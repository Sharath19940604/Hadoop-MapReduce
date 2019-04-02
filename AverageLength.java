package hello;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageLength {
	
		public static void main(String[] args) throws Exception {

		if (args.length != 2) {
		System.err.println("Usage: WordCount <input path> <Output path>");
		System.exit(-1); }

		System.out.println("In Driver now!");

		Job job = Job.getInstance();
		job.setJarByClass(AverageLength.class);
		job.setJobName("WordCount");
		job.setMapperClass(AverageMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[0]),
				TextInputFormat.class, AverageMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),
				TextInputFormat.class, AverageMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[2]),
				TextInputFormat.class, AverageMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[3]),
				TextInputFormat.class, AverageMapper.class);
		
		
		
		
		job.setReducerClass(Avg_Reducer.class);
	

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

	


