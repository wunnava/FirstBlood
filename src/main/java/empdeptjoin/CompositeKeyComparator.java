/**
 * SortComparator (Custom comparator)
 * basically sorts keys. The same logic
 * is used in the default(natural sort order) key class.
 * So it delegates its compare call to the compareTo
 * of the Comparable type (ActualKey). 
 * I think the reason why we need it in the first place
 * is that Hadoop framework needs a CustomComparator 
 * to work with sorting keys.
 * 
 *  Sorting happens twice. 
 *  1. in-memory sorting of partitions in spill files.
 *  2. in the reduce phase after merging the the disk 
 *  files based on partitions.
 *  
 *  On the other hand, group comparator (again another
 *  custom comparator on key) is required to group keys
 *  which groups based on First Element and not the entire
 *  key (unlike default compareTo)
 * 
 */

package empdeptjoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
public class CompositeKeyComparator extends WritableComparator {
protected CompositeKeyComparator() {
super(CompositeKey.class, true);
}
@SuppressWarnings("rawtypes")
@Override
public int compare(WritableComparable w1, WritableComparable w2) 
{

CompositeKey key1 = (CompositeKey) w1;
CompositeKey key2 = (CompositeKey) w2;
 
// (first check on udid)
int compare = key1.getDeptId().compareTo(key2.getDeptId());
 
if (compare == 0) 
{
// only if we are in the same input group should we try and sort by value (datetime)
return key1.getSrcId().compareTo(key2.getSrcId());
}
 
return compare;
}
}