import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForWC extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context context)
			throws IOException, InterruptedException {
		int total=0;
		for(IntWritable i: arg1 )
		{
			total+=i.get();
		}
		context.write(arg0, new IntWritable(total));
	}

	
}
