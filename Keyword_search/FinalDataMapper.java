/*****
Author: Amita Ghosh
Date: 12th March 2021
Description: Mapper Class for dataset one
References:https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
			https://www.codota.com/code/java/classes/org.apache.hadoop.util.GenericOptionsParser
			https://github.com/learninghadoop2/book-examples/blob/master/ch3/src/main/java/com/learninghadoop2/mapreduce/WordCount.java
****/
package finalDatasetCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text wordToken = new Text(); // output key from mapper
	//map() function
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		wordToken.set("JoinedSetCount");
		if(value.toString().toLowerCase().contains("end of the record")) { // check if  'end of the record' is present at the end to identify each record easily.
			context.write(wordToken, new IntWritable(1)); //write the output
		}
	}

}
