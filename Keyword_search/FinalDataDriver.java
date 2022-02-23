/*****
Author: Amita Ghosh
Date: 12th March 2021
Description: Main Class/Driver Class

References:https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
			https://www.codota.com/code/java/classes/org.apache.hadoop.util.GenericOptionsParser
			https://github.com/learninghadoop2/book-examples/blob/master/ch3/src/main/java/com/learninghadoop2/mapreduce/WordCount.java
****/
package finalDatasetCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class FinalDataDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] pathArgs = null;
		// handle the arguments given in the command
		try {
			pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (pathArgs.length < 2)
		{
			System.err.println("Not enough arguments in the command");
			System.exit(2);
		}
		
		//create job
		Job countJob = Job.getInstance(conf,"Count job");
		countJob.setJarByClass(FinalDataDriver.class);
		countJob.setReducerClass(FinalDataReducer.class);
		countJob.setMapOutputKeyClass(Text.class);
		countJob.setMapOutputValueClass(IntWritable.class);
		MultipleInputs.addInputPath(countJob, new Path(args[0]),TextInputFormat.class, FinalDataMapper.class);
		
	    // recurse through subfolders also
		FileInputFormat.setInputDirRecursive(countJob, true);

		// output file path
		FileOutputFormat.setOutputPath(countJob, new Path(pathArgs[pathArgs.length-1]));
		System.exit(countJob.waitForCompletion(true) ? 0 : 1);

	}

}
