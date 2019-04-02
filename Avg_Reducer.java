package hello;

import java.awt.List;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Avg_Reducer extends Reducer<Text,IntWritable, Text , 
IntWritable> {
// access modifier as per our need 
public Map<String , Integer > map = new LinkedHashMap<String , Integer>();
public void reduce(Text key , Iterable<IntWritable> values ,Context context 
)
{
int languageCount=0;
int languageHits=0;

//logic for the reducer 
//reduced values in map for each key
for (IntWritable value : values ){
	languageCount++;
	languageHits += value.get();
	}


	


map.put(key.toString(),new Integer(languageHits/languageCount)); 


	
   }

    public void cleanup(Context context) throws IOException, InterruptedException{ 
  //Cleanup is called once at the end to finish off anything for reducer
  //Here we will write our final output
  Map<String , Integer>  sortedMap = new HashMap<String , Integer>();

 
sortedMap = sortMap(map);

  for (Map.Entry<String,Integer> entry : sortedMap.entrySet())
  {
  context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
    }


   }
   public Map<String , Integer > sortMap (Map<String,Integer> unsortMap){

Map<String ,Integer> hashmap = new LinkedHashMap<String,Integer>();
 int count=0;
 LinkedList<Map.Entry<String,Integer>> list = new LinkedList<Map.Entry<String,Integer>>(unsortMap.entrySet());
//Sorting the list created from unsorted Map
 Collections.sort(list , new Comparator<Map.Entry<String,Integer>>(){

  public int compare (Map.Entry<String , Integer> o1 , Map.Entry<String , 
       Integer> o2 ){
      //sorting in descending order
      return o2.getValue().compareTo(o1.getValue());

  }


});

 for(Map.Entry<String, Integer> entry : list){
  // only writing top 3 in the sorted map 
    if(count>2)
      break;

    hashmap.put(entry.getKey(),entry.getValue());


}

return hashmap ;

}

}

