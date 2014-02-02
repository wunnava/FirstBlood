package uniqueURL;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForUniqueURL extends Mapper<LongWritable, Text,Text, Text>
{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		String[] myarr=line.split(",");
		//if(!(myarr[0].trim().equals("")|| myarr[1].trim().equals("")))
		context.write(new Text(myarr[0]), new Text(myarr[1]));
				
	}

	
	
}
