import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import javax.json.*;
import java.io.StringReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.Iterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.Comparator;
import java.util.LinkedHashMap;

public class Twitter {	

	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input1 = "/class/s16419x/lab5/oscars.json";
		//String temp1 = "/scr/mattrose/lab5/temp1";
		//String temp2 = "/scr/mattrose/lab5/temp2";
		String output = "/scr/mattrose/lab5/exp3/output";
		int reduce_tasks = 1;

		Job job_one = Job.getInstance(conf, "Twitter Program Round One");
		job_one.setJarByClass(Twitter.class);
		job_one.setNumReduceTasks(reduce_tasks);

		//job_one.setPartitionerClass(CustomPartitioner.class);

		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);

		job_one.setMapperClass(Map_One.class);
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(MyJSONInputFormat.class);
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input1));
		FileOutputFormat.setOutputPath(job_one, new Path(output));

		job_one.waitForCompletion(true);


		//Job job_two = Job.getInstance(conf, "Sorting Program Round Two");
		//job_two.setJarByClass(Patent.class);
		//job_two.setNumReduceTasks(reduce_tasks);
		
		//job_two.setMapOutputKeyClass(IntWritable.class);
		//job_two.setMapOutputValueClass(IntWritable.class);
		//job_two.setOutputKeyClass(IntWritable.class);
		//job_two.setOutputValueClass(IntWritable.class);

		//job_two.setMapperClass(Map_Two.class);
		//job_two.setReducerClass(Reduce_Two.class);

		//job_two.setInputFormatClass(TextInputFormat.class);
		//job_two.setOutputFormatClass(TextOutputFormat.class);

		//FileInputFormat.addInputPath(job_two, new Path(temp1));
		//FileOutputFormat.setOutputPath(job_two, new Path(temp2));

		//job_two.waitForCompletion(true);

		
		//Job job_three = Job.getInstance(conf, "Sorting Program Round Three");
		//job_three.setJarByClass(Patent.class);
		//job_three.setNumReduceTasks(reduce_tasks);
		
		//job_three.setMapOutputKeyClass(IntWritable.class);
		//job_three.setMapOutputValueClass(IntWritable.class);
		//job_three.setOutputKeyClass(IntWritable.class);
		//job_three.setOutputValueClass(IntWritable.class);

		//job_three.setMapperClass(Map_Three.class);
		//job_three.setReducerClass(Reduce_Three.class);

		//job_three.setInputFormatClass(TextInputFormat.class);
		//job_three.setOutputFormatClass(TextOutputFormat.class);

		//FileInputFormat.addInputPath(job_three, new Path(temp2));
		//FileOutputFormat.setOutputPath(job_three, new Path(output));

		//job_three.waitForCompletion(true);

	}

	
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {
		
		private JSONParser parser = new JSONParser();

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

			try{
				Object obj = parser.parse(value.toString());
				JSONObject tweet = (JSONObject) obj;
				JSONObject user = (JSONObject) tweet.get("user");				
				String screen_name = (String) user.get("screen_name");
				context.write(new Text(screen_name), new Text(tweet.toString()));
			}
			catch (ParseException e)
			{
				e.printStackTrace();
			}
		}
	
	}
		

	public static class Reduce_One extends Reducer<Text, Text, Text, Text>{
	
		private HashMap topTweeters = new HashMap<>();
		private HashMap topHashtags = new HashMap<>();

		private JSONParser parser = new JSONParser();

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			
			try{	
				ArrayList<String> hashtagList = new ArrayList<String>();
				long tweets = 0;
				for( Text val : values )
				{
					Object obj = parser.parse(val.toString());
					JSONObject tweet = (JSONObject) obj;
					JSONObject user = (JSONObject) tweet.get("user");
					tweets = (long) user.get("statuses_count");
					JSONObject entities = (JSONObject) tweet.get("entities");
					JSONArray hashtags = (JSONArray) entities.get("hashtags");
					Iterator<JSONObject> it = hashtags.iterator();
					while( it.hasNext() )
					{
						String hashtag = (String) it.next().get("text");
						hashtagList.add(hashtag);
					}
				}
				topTweeters.put(key.toString(), tweets);
				topHashtags.put(key.toString(), hashtagList);
			}
			catch (ParseException e)
			{
				e.printStackTrace();
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			HashMap sortedMap = new HashMap(sortByValues(topTweeters));

			int counter = 0;
			for( String key : (Set<String>) sortedMap.keySet() )
			{
				if( counter++ == 10 )
				{
					break;
				}
				int oldCount = 0;
				String hashtagText = "";
				HashSet<String> unique = new HashSet<String>((ArrayList)topHashtags.get(key));
				for( String hashtag : unique )
				{
					int newCount = Collections.frequency((ArrayList)topHashtags.get(key), hashtag);
					if( newCount > oldCount )
					{
						oldCount = newCount;
						hashtagText = hashtag;
					}
				}
				context.write(new Text(key), new Text(hashtagText));
			}

		}


		private static HashMap sortByValues(HashMap map)
		{
			List list = new LinkedList(map.entrySet());

			Collections.sort(list, new Comparator() {
				public int compare(Object o1, Object o2) {
					return ((Comparable)((Map.Entry)(o1)).getValue()).compareTo(
						((Map.Entry) (o2)).getValue());
				}
			});

			HashMap sortedHashMap = new LinkedHashMap();
			for( Iterator it = list.iterator(); it.hasNext(); ) {
				Map.Entry entry = (Map.Entry) it.next();
				sortedHashMap.put(entry.getKey(), entry.getValue());
			}
			return sortedHashMap;
		}
	}

	
	public static class MyJSONInputFormat extends FileInputFormat<LongWritable, Text> {

		public RecordReader<LongWritable, Text> createRecordReader( InputSplit split, TaskAttemptContext context )
			throws IOException, InterruptedException {

			return new MyJSONRecordReader();

		}

	}


	public static class MyJSONRecordReader extends RecordReader<LongWritable, Text> {

		private long start;
		private long end;
		private long pos;
		private LineReader in;
		private int maxLineLength;

		private LongWritable key = new LongWritable();
		private Text value = new Text();

		private boolean firstFlag = true;

		public void close() throws IOException {
			if( in != null ) 
			{
				in.close();
			}
		}

		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		public float getProgress() throws IOException, InterruptedException {
			if( start == end )
			{
				return 0.0f;
			}
			else
			{
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		public void initialize(InputSplit inputSplit, TaskAttemptContext context) 
			throws IOException, InterruptedException {
	
			FileSplit split = (FileSplit) inputSplit;

			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

			start = split.getStart();
			end = start + split.getLength();

			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);

			FSDataInputStream fileIn = fs.open(file);

			in = new LineReader(fileIn, job);
			Text line = new Text();

			pos = start;
			int offset;
			boolean flag = false;
			while( flag == false )
			{
				line.clear();
				offset = in.readLine(line);
				String str = line.toString();
				if( str.equals(" {") )
				{
					flag = true;
				}
				else
				{
					pos += offset;
				}
			}
			
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {

			key.set(pos);

			int newSize = 0;
			Text line = new Text();
			String jsonObj = "";
		
			int braceCount = 0;

			if( firstFlag == true )
			{
				jsonObj = " {";
				braceCount = 1;
				firstFlag = false;
			}

			while( pos < end )
			{
				line.clear();

				newSize = in.readLine(line, maxLineLength,
						Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
				
				String str = line.toString();

				String trimStr = str.trim();

				if( trimStr.equals("][") ) 
				{
					continue;
				}

				if( trimStr.startsWith("}") )
				{
					braceCount--;
				}
				else if( trimStr.endsWith("{") )
				{
					braceCount++;
				}

				jsonObj += str;
				pos += newSize;

				if( trimStr.startsWith("}") && braceCount == 0 )
				{
					int last = jsonObj.lastIndexOf("}");
					value.set(jsonObj.substring(0, last + 1));
					return true;
				}
			}

			return false;
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
