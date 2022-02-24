/*****
Author: Amita Ghosh
Date: 12th March 2021
Description: Reducer class
References:https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
			https://www.codota.com/code/java/classes/org.apache.hadoop.util.GenericOptionsParser
			https://github.com/learninghadoop2/book-examples/blob/master/ch3/src/main/java/com/learninghadoop2/mapreduce/WordCount.java
****/
package finalDatasetCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalDataReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
	IntWritable count = new IntWritable();// output value 
	
	//reduce() function
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		// count the total number of records 
		int valueSum = 0;
		for (IntWritable val : values) {
			valueSum += val.get();
		}
		count.set(valueSum);
		context.write(key, count);
	}
}
