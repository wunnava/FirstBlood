package empsalarystats;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForEmpSal extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException
			{
		// TODO Auto-generated method stub
		String line = value.toString();
		String [] myarr=line.split(",");
		
			//word=word.trim();
			context.write(new Text(myarr[2]), value);
		

			}

}
