package hello;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static enum MyCounters{Invalid,Missing}
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {

String s = value.toString();
	String[] page= s.split("\n");
	for (int i=0;i<page.length;i++)
	{
		StringBuffer lrn=new StringBuffer(page.length);
	String[] lineArr=page[i].split("\\s+");
	if(lineArr[0].contains(".")){
		context.getCounter(MyCounters.Invalid).increment(1);
	}else{
	context.write(new Text(lineArr[1].concat(lrn.toString())),new IntWritable(Integer.parseInt(lineArr[2])));
	}
	}
}	
}

	
	


