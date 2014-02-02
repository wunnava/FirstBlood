package empgroups;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForEmpGroups extends Reducer<IntWritable, Text, IntWritable, Text>
{

	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		
		Iterator<Text> it= arg1.iterator();
		while(it.hasNext())
		{
			arg2.write(arg0, new Text(it.next())); 
		}
		
	}

}
