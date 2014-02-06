package empdeptDC;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This key is a composite key. The "actual"
 * key is the deptID. The secondary sort will be performed against the srcid.
 */
public class CompositeKey implements WritableComparable {

	private String deptid;
	private String srcid;

	public CompositeKey() {
	}

	public CompositeKey(String deptid, String srcid) {

		this.deptid = deptid;
		this.srcid = srcid;
	}

	@Override
	public String toString() {

		return (new StringBuilder()).append(deptid).append(',').append(srcid).toString();
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		CompositeKey key2=(CompositeKey)o;
		int result=0;
		if(Integer.parseInt(this.getDeptId())<Integer.parseInt(key2.getDeptId()))
		{
			result= -1;
		}
		else if(Integer.parseInt(this.getDeptId())==Integer.parseInt(key2.getDeptId()))
		{
			if(Integer.parseInt(this.getSrcId())<Integer.parseInt(key2.getSrcId()))
			{
				result= -1;
			}

			else if(Integer.parseInt(this.getSrcId())==Integer.parseInt(key2.getSrcId()))
			{
				result= 0;
			}
			else if(Integer.parseInt(this.getSrcId())>Integer.parseInt(key2.getSrcId()))
			{
				result= 1;
			}
		}
		else if(Integer.parseInt(this.getDeptId())<Integer.parseInt(key2.getDeptId()))
		{
			result= 1;
		}

		return result;
	} 
	//@Override
	//de-serialize the fields of this object from in.
	public void readFields(DataInput in) throws IOException {

		deptid = WritableUtils.readString(in);
		srcid = WritableUtils.readString(in);
	}

	//@Override
	//Serialize the fields of this object to out.
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, deptid);
		WritableUtils.writeString(out, srcid);
	}



	/**
	 * Gets the udid.
	 *
	 * @return UDID.
	 */
	public String getDeptId() {

		return deptid;
	}

	public void setDeptId(String deptid) {

		this.deptid = deptid;
	}

	/**
	 * Gets the datetime.
	 *
	 * @return Datetime
	 */
	public String getSrcId() {

		return srcid;
	}

	public void setSrdId(String srcid) {

		this.srcid = srcid;
	}


}