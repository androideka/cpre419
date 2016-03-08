import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GCC {

	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input1 = "/class/s16419x/lab3/patents.txt";
		String temp1 = "/scr/mattrose/lab3/exp2/temp1";
		String temp2 = "/scr/mattrose/lab3/exp2/temp2";
		String output = "/scr/mattrose/lab3/exp2/output";
		int reduce_tasks = 4;

		Job job_one = Job.getInstance(conf, "GCC Program Round One");
		job_one.setJarByClass(Patent.class);
		job_one.setNumReduceTasks(reduce_tasks);

		job_one.setMapOutputKeyClass(IntWritable.class);
		job_one.setMapOutputValueClass(Text.class);
		job_one.setOutputKeyClass(IntWritable.class);
		job_one.setOutputValueClass(IntWritable.class);

		job_one.setMapperClass(Map_One.class);
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(TextInputFormat.class);
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input1));
		FileOutputFormat.setOutputPath(job_one, new Path(temp1));

		job_one.waitForCompletion(true);


		Job job_two = Job.getInstance(conf, "GCC Program Round Two");
		job_two.setJarByClass(Patent.class);
		job_two.setNumReduceTasks(reduce_tasks);
		
		job_two.setMapOutputKeyClass(IntWritable.class);
		job_two.setMapOutputValueClass(IntWritable.class);
		job_two.setOutputKeyClass(IntWritable.class);
		job_two.setOutputValueClass(IntWritable.class);

		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_two, new Path(temp1));
		FileOutputFormat.setOutputPath(job_two, new Path(temp2));

		job_two.waitForCompletion(true);

		
		Job job_three = Job.getInstance(conf, "GCC Program Round Three");
		job_three.setJarByClass(Patent.class);
		job_three.setNumReduceTasks(reduce_tasks);
		
		job_three.setMapOutputKeyClass(IntWritable.class);
		job_three.setMapOutputValueClass(IntWritable.class);
		job_three.setOutputKeyClass(IntWritable.class);
		job_three.setOutputValueClass(IntWritable.class);

		job_three.setMapperClass(Map_Three.class);
		job_three.setReducerClass(Reduce_Three.class);

		job_three.setInputFormatClass(TextInputFormat.class);
		job_three.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_three, new Path(temp2));
		FileOutputFormat.setOutputPath(job_three, new Path(output));

		job_three.waitForCompletion(true);

	}

	
	public static class Map_One extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

			String[] vals = value.toString().split("[\\s]+");

		}

	}
	

	public static class Reduce_One extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		}
	}


	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

			String[] vals = value.toString().split("[\\s]+");

		}

	}


	public static class Reduce_Two extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

		}

	}


	public static class Map_Three extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
			String[] vals = value.toString().split("[\\s]+");

		}

	}


	
	public static class Reduce_Three extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		}

	}
}
