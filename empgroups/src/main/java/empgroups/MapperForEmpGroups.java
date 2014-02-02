package empgroups;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForEmpGroups extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line=value.toString();
		String[] splits=line.split(",");
		int i=0;
		try{
		i=Integer.parseInt(splits[2].trim());
		}
		catch(Exception e)
		{
			
		}
		if (i>30)
		context.write(new IntWritable(30), value);
		
	}

}
