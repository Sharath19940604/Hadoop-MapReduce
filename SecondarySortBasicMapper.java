package hello;





import hello.WordMapper.MyCounters;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortBasicMapper extends
  	Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String s = value.toString();
		String[] page= s.split("\n");
			for (int i=0;i<page.length;i++)
			{
				String[] lineArr=page[i].split("\\s+");
				if(lineArr[0].contains(".")){
					context.getCounter(MyCounters.Invalid).increment(1);
				}else{
			context.write(
					new CompositeKeyWritable(lineArr[1].toString(),(lineArr[2].toString())), NullWritable.get());
		
				}
}
	}
}