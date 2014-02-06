package union;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverForUnion extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new DriverForUnion(), args));
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		if(arg0.length != 2) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
            return -1;
		}
		@SuppressWarnings("deprecation")
		Job job= new Job(getConf(),"job for union");
		job.setJarByClass(DriverForUnion.class);
		job.setJobName("Union for Movies");
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		job.setMapperClass(MapperForMovies1.class);
		job.setReducerClass(ReducerForMovies.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		if(job.waitForCompletion(true)) 
		{
			return 0;
		}
		return 1;
	}

	}
