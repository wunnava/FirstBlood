package uniqueURL;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForUniqueURL extends Reducer<Text, Text, Text, IntWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		
		HashSet<String> set= new HashSet<String>();
		Iterator<Text> it= arg1.iterator();
		while(it.hasNext())
		{
		 set.add(it.next().toString());
		}
		arg2.write(arg0, new IntWritable(set.size()));
	}

}
