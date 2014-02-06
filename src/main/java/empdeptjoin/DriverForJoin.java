package empdeptjoin;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverForJoin extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length != 3) {
			System.out.printf("Usage: %s [generic options] <input dir> <input dir> <output dir>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		
	     
	     // Specify various job-specific parameters     
	    
		@SuppressWarnings("deprecation")
		Job job=new Job(getConf(),"Joining Emp with Dept");
		job.setJarByClass(DriverForJoin.class);
		
		Path empInputPath=new Path(args[0]);
		Path deptInputPath=new Path(args[1]);
		Path outputPath=new Path(args[2]);
		
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job, empInputPath, TextInputFormat.class,MapperForEmp.class);
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job, deptInputPath, TextInputFormat.class,MapperForDept.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setReducerClass(ReducerForJoin.class);
		job.setPartitionerClass(ActualKeyPartitioner.class);
		job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setOutputKeyClass(Text.class);

		if(job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new DriverForJoin(), args));

	}

}

/*
 *  hadoop jar JarForJoin.jar empdeptjoin.DriverForJoin InputJoinEmp InputJoinDept outputJoin2

 * 10	1224,Venkata,10
10	1233,Arun,10
10	1229,Srikanth,10
10	1226,Nandhan,10
10	1231,Varun,10
10	10,Admin
20	1228,Avinash,20
20	1225,Satya,20
20	1226,Prakash,20
20	1225,Wunnava,20
20	1227,Bharat,20
20	1230,RaviTeja,20
20	1232,Santosh,20
20	1234,Hanish,20
20	20,Development
*/
