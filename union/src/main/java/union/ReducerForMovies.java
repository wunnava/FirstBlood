package union;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForMovies extends Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> itable,Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> it=itable.iterator();
		context.write(key, it.next());
	}

	
}
