package empsalarystats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class DriverForEmpSal extends Configured implements Tool
{
	public static void main(String args[])
	{
		try {
			System.exit(ToolRunner.run(new DriverForEmpSal(), args));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length != 2) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
            return -1;
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		JobConf jobconf = new JobConf(new Configuration(), DriverForEmpSal.class);
		jobconf.setJarByClass(DriverForEmpSal.class);
		//jobconf.setQueueName("testing");
		job.setJarByClass(DriverForEmpSal.class);
		job.setJobName(getClass().getName());
				
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapperForEmpSal.class);
		job.setReducerClass(ReducerForEmpSal.class);
		//job.setCombinerClass(ReducerForEmpSal.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		if(job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

}
