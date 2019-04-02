package hello;




import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class CompositeKeyWritable implements Writable,
  	WritableComparable<CompositeKeyWritable> {

	private String name;
	private String hits;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String name, String hits) {
		this.name = name;
		this.hits = hits;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(name).append("\t")
				.append(hits)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		name = WritableUtils.readString(dataInput);
		hits = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, name);
		WritableUtils.writeString(dataOutput, hits);
	}

	public int compareTo(CompositeKeyWritable objKeyPair) {
		
		int result = name.compareTo(objKeyPair.name);
		if (0 == result) {
			result = hits.compareTo(objKeyPair.hits);
		}
		return result;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getHits() {
		return hits;
	}

	public void setHits(String hits) {
		this.hits = hits;
	}
}