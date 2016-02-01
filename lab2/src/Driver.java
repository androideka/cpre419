import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Stack;
import java.util.StringTokenizer;

import com.sun.org.apache.xpath.internal.SourceTree;
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

public class Driver {
	
	public static void main (String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// Change input and output paths accordingly or pass arguments from command line
		String input1 = "/class/s16419x/lab2/shakespeare";
		String input2 = "/class/s16419x/lab2/gutenberg";
		String temp = "/scr/mattrose/lab2/exp2temp";
		String output = "/scr/mattrose/lab2/exp2output/";
		
		int reduce_tasks = 4;  // The number of reduce tasks that will be assigned to the job
		
		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Driver Program Round One");
		
		// Attach the job to this Driver
		job_one.setJarByClass(Driver.class); 
		job_one.setNumReduceTasks(reduce_tasks);
		
		// Set the output key-value pairs for Mapper and Reducer
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(IntWritable.class);
		job_one.setOutputKeyClass(Text.class); 
		job_one.setOutputValueClass(IntWritable.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		
		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input1));
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = Job.getInstance(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(Driver.class); 
		job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(IntWritable.class);
		
		// If required, the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_two, new Path(output));
		
		// Run the job
		job_two.waitForCompletion(true); 
		
		/**
		 * **************************************
		 * **************************************
		 * FILL IN CODE FOR MORE JOBS IF YOU NEED
		 * **************************************
		 * **************************************
		 */
		
	} // End run
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable>  {

		private Text bigram = new Text();
		private final static IntWritable one = new IntWritable(1);
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			line = line.toLowerCase();
			
			// Tokenize to get the individual words
			StringTokenizer tokens = new StringTokenizer(line, " 0123456789\t\\s+.!?,;:'\"()[]{}&-/");
			ArrayList<String> words = new ArrayList<>();
			while (tokens.hasMoreTokens()) {
				
				/**
				 * ***********************************
				 * ***********************************
				 * FILL IN CODE FOR THE MAP FUNCTION
				 * ***********************************
				 * ***********************************
				 */
				words.add(tokens.nextToken());

			} // End while
			
			/**
			 * ***********************************
			 * ***********************************
			 * FILL IN CODE FOR THE MAP FUNCTION
			 * ***********************************
			 * ***********************************
			 */
			Stack<String> bigrams = new Stack<>();

			for(int i = 1; i < words.size(); i++)
			{
				bigrams.add(words.get(i-1) + " " + words.get(i));
			}

			// Use context.write to emit values
			while(!bigrams.isEmpty())
			{
				bigram.set(bigrams.pop());
				context.write(bigram, one);
			}


		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable>  {
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fashion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
											throws IOException, InterruptedException  {
			int sum = 0;
			for (IntWritable val : values) {
				int value = val.get();
				
				/**
				 * **************************************
				 * **************************************
				 * YOUR CODE HERE FOR THE REDUCE FUNCTION
				 * **************************************
				 * **************************************
				 */

				sum += value;
			}
			
			/**
			 * **************************************
			 * **************************************
			 * YOUR CODE HERE FOR THE REDUCE FUNCTION
			 * **************************************
			 * **************************************
			 */
			context.write(key, new IntWritable(sum));
			
			// Use context.write to emit values
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {
		private Text firstLetter = new Text();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException  {
			
			
			/**
			 * ***********************************
			 * ***********************************
			 * FILL IN CODE FOR THE MAP FUNCTION
			 * ***********************************
			 * ***********************************
			 */
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t\\s+");
			String firstWord = tokenizer.nextToken();
			firstLetter.set(Character.toString(firstWord.charAt(0)));
			context.write(firstLetter, value);
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable>  {
		private Text highBigram = new Text();
		private IntWritable highCount = new IntWritable(0);
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			/**
			 * **************************************
			 * **************************************
			 * YOUR CODE HERE FOR THE REDUCE FUNCTION
			 * **************************************
			 * **************************************
			 */
			int high = 0;
			for(Text val : values)
			{
				String line = val.toString();
				StringTokenizer tokenizer = new StringTokenizer(line, " \t\\s+");
				String bigram = "";
				bigram += tokenizer.nextToken() + " ";
				bigram += tokenizer.nextToken();
				int count = Integer.parseInt(tokenizer.nextToken());
				if(count > high)
				{
					highBigram.set(bigram);
					highCount.set(count);
					high = count;
				}
			}
			context.write(highBigram, highCount);
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	

	/**
	 * ******************************************************
	 * ******************************************************
	 * YOUR CODE HERE FOR MORE MAP / REDUCE CLASSES IF NEEDED
	 * ******************************************************
	 * ******************************************************
	 */
	
}