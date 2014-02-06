package union;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForMovies1 extends Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
					throws IOException, InterruptedException 
					{
		String record= value.toString();
		if(record.trim().equals("")){return;}
		String[] myarr=record.split(",");
		
		context.write(new IntWritable(Integer.parseInt(myarr[0])), value);
		

					}




}
