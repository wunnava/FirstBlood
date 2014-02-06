package empdeptDC;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForEmp extends Mapper<LongWritable, Text, CompositeKey, Text>
{
	HashMap<Integer,String> deptMap=new HashMap<Integer,String>();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] myarr= value.toString().split(",");
		CompositeKey actualkey= new CompositeKey(myarr[2].trim(),"0");
		String deptInfo=deptMap.get(Integer.parseInt(myarr[2].trim()));
		String finalRec=value.toString()+deptInfo;
		context.write(actualkey, new Text(finalRec));
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.setup(context);
		ParseDataFile df=new ParseDataFile();
		deptMap=df.parseFile("dept.txt");
		
		
	}

}
class ParseDataFile
{
	public HashMap<Integer,String> parseFile(String filename)
	{
		HashMap<Integer, String> deptMap=new HashMap<Integer,String>();
		try{
		File myfile=new File(filename);
		BufferedReader br= new BufferedReader(new FileReader(myfile));
		String s=null;
		while((s=br.readLine())!=null)
		{
			String myarr[]=s.split(",");
			deptMap.put(Integer.parseInt(myarr[0].trim()),myarr[1].trim());
		 //deptList.add(s);	
		}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return deptMap;
		
	}
}