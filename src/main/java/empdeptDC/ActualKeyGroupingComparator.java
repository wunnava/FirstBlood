package empdeptDC;


/**
 * GroupComparator is used to define what  keys gets
 * its corresponding values grouped for a reducer.
 * 
 * For eg: composite key(10,0 : 10,1 : 10,1 : 10,1 : 10,1)
 * should have all its values go to the same reducer.
 * If we implement the compare such that it checks the
 * second element also then, 10,0 will be sent as one key
 * with just one value in its iterator whereas 10,1 will 
 * be sent in the next input wave with all its values 
 * in the iterable value.
 */
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ActualKeyGroupingComparator extends WritableComparator {
 
protected ActualKeyGroupingComparator() {
 
super(CompositeKey.class, true);
}
 
@SuppressWarnings("rawtypes")
@Override
public int compare(WritableComparable w1, WritableComparable w2) {
 
CompositeKey key1 = (CompositeKey) w1;
CompositeKey key2 = (CompositeKey) w2;
 
// (check on udid)
return key1.getDeptId().compareTo(key2.getDeptId());
}
}
