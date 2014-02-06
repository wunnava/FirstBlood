package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class MapperForWC extends Mapper<LongWritable,Text , Text, IntWritable> 
{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String [] myarr=line.split("\\W+");
		for(String word: myarr)
		{
			word=word.trim();
			context.write(new Text(word), new IntWritable(1));
		}
	}
		

}
