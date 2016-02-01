/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A simple Hadoop MapReduce program to count the number of occurrence of all
 * distinct words. Source code from:
 * https://github.com/apache/hadoop/blob/branch-2.4.1/hadoop-mapreduce-project/
 * hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/WordCount.
 * java
 * 
 */

public class WordCount {
	
	public static void main(String[] args) throws Exception {
		
	    Configuration conf = new Configuration();
	    
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
		
		// Set number of reduce tasks
		int reduce_tasks = 4;
		
		// Create a Hadoop Job
		Job job = Job.getInstance(conf, "word count");
		
		// Attach the job to this Class
		job.setJarByClass(WordCount.class);
		
		// Number of reducers
		job.setNumReduceTasks(reduce_tasks);
		
		// Set the Map class
		job.setMapperClass(TokenizerMapper.class);
		
		// Set the Combiner class
        // The combiner class reduces the mapper output locally
        // All the outputs from the mapper having the same key are reduced locally
        // This helps in reducing communication time as reducers get only
        // one tuple per key per mapper
        // For this example, the Reduce logic is good enough as the combiner logic
        // Hence we use the same class. 
        // However, this is not necessary and you can write separate Combiner class
		// job.setCombinerClass(IntSumReducer.class);
		
		// Set the reducer class
		job.setReducerClass(IntSumReducer.class);
		
		// Set how the input is split
        // TextInputFormat.class splits the data per line
		job.setInputFormatClass(TextInputFormat.class); 
        
        // Output format class
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set the Output Key from the mapper
		job.setMapOutputKeyClass(Text.class);
		
		// Set the Output Value from the mapper
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set the Output Key from the reducer
		job.setOutputKeyClass(Text.class);
		
		// Set the Output Value from the reducer
		job.setOutputValueClass(IntWritable.class);

		// Input path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// Output path
		// The directory shouldn't exist before running the job because
		// Hadoop will complain and not run the job. This precaution is to prevent data loss (it can
		// be very annoying to accidentally overwrite the output of a long job with that of another)
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Run the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text(String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and
	// IntWribale as value
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		 // The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// The TextInputFormat splits the data line by line.
            // So each map method receives one line from the input
			String line = value.toString();
                                
            // Tokenize to get the individual words
			// By default, it is the "\t\n\r\f"
			StringTokenizer itr = new StringTokenizer(line);
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

}