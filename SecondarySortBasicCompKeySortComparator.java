package hello;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortBasicCompKeySortComparator extends WritableComparator {

  protected SecondarySortBasicCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

	
		Integer a=Integer.parseInt(key1.getHits());
		Integer b=Integer.parseInt(key2.getHits());
			return -a.compareTo(b);
			
	}
}
