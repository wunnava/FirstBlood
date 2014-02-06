package empsalarystats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForEmpSal extends Reducer<Text, Text, Text, Text>
{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Iterator<Text> it= arg1.iterator();
		String currRecord = null;
		double avgSal=0;
		int maxSal=0,minSal=0,count=0;
		double totalSal=0;
		
		while(it.hasNext())
		{			
		 currRecord= it.next().toString().trim();
		 if(currRecord.equals(""))break;
		 String[] myarr=currRecord.split(",");
		 //calulate total and avg sal
		 totalSal+=Integer.parseInt(myarr[4].trim());
		 //count of employees with same salaries
		 count++;
		 
		 //minsal setup
		 if(count==1)
			 minSal=Integer.parseInt(myarr[4].trim());
		 else{
			 if(Integer.parseInt(myarr[4].trim())<minSal)
			 {
				 minSal=Integer.parseInt(myarr[4].trim());
			 }
		 }
		 //max sal setup
		 if(Integer.parseInt(myarr[4].trim())>maxSal)
		 {
			 maxSal=Integer.parseInt(myarr[4].trim());
		 }
		 
		 //arg2.write(arg0, new Text(currRecord.toString()+count));
		 
		}
		String record="";
		if (count>0)
		{
			avgSal=totalSal/count;
			record= new String(count+" employees of age "+arg0.toString()+
					" with an average salary of "+avgSal+". Minimum salary = "+minSal+
					" and Maximum sal = "+maxSal);

		}
		arg2.write(arg0, new Text(record));
				
	
	}
 
	
}
