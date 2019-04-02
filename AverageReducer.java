package hello;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{

public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
int Language = 0;
int hits=0;
System.out.println("In Reducer now!");

for (IntWritable value : values) {
Language++;
hits += value.get();
}
context.write(key, new IntWritable(hits/Language));
}
}






