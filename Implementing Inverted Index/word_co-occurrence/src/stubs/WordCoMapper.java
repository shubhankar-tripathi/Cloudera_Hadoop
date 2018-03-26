package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * TODO implement
     */
	  String[] line = value.toString().split("\\W+");
	    StringBuilder phrase = new StringBuilder();
	    boolean firstLoop = true;
	    for(String word : line)
	    {
	      if (word.length() > 0)
	      {
	    	  if(firstLoop){
	    		  firstLoop = false;
	    		  phrase.append(word.toLowerCase());
	    		  continue;
	    	  }
	    	  phrase.append(',');
	    	  phrase.append(word.toLowerCase());
	    	  context.write(new Text(phrase.toString()), new IntWritable(1));
	    	  phrase.delete(0, phrase.length());
	    	  phrase.append(word.toLowerCase());
	    		  
	    	  }
	      }
    
  }
}
