package wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerForWC  extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context)
					throws IOException, InterruptedException {
		Iterator it= value.iterator();
		int total = 0;
		while (it.hasNext()) {
			total+=(Integer) it.next();
		}
		context.write(key, new IntWritable(total));
	}

}
