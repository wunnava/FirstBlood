package empdeptjoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForDept extends Mapper<LongWritable, Text, CompositeKey, Text>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] myarr=value.toString().split(",");

		CompositeKey actualkey = new CompositeKey(myarr[0], "1");
		context.write(actualkey, value);
	}

}
