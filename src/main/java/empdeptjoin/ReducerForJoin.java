package empdeptjoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForJoin extends Reducer<CompositeKey, Text, Text, Text>{

	@Override
	protected void reduce(CompositeKey arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		Iterator<Text> it=arg1.iterator();
		while(it.hasNext())
		arg2.write(new Text(arg0.getDeptId()), it.next());
	}

}
