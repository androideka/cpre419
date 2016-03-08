import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.Iterator;

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

public class Patent {

	private final static int NUM_TOP_PATENTS = 10;

	private static class PatentComparator implements Comparator<PatentSum>{
		
		@Override
		public int compare(PatentSum o1, PatentSum o2){
			return ((Integer)o1.getCitations()).compareTo((Integer)o2.getCitations());
		}

	};

	private static ArrayList<PatentSum> top_patents = new ArrayList<PatentSum>();
	private static int min_patent_citations = 10000;

	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input1 = "/class/s16419x/lab3/patents.txt";
		String temp1 = "/scr/mattrose/lab3/exp1/temp1";
		String temp2 = "/scr/mattrose/lab3/exp1/temp2";
		String output = "/scr/mattrose/lab3/exp1/output";
		int reduce_tasks = 4;

		for(int i = 0; i < 10; i++){
		
			PatentSum patent = new PatentSum(0, 0);
			top_patents.add(patent);
		}

		Job job_one = Job.getInstance(conf, "Patents Program Round One");
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


		Job job_two = Job.getInstance(conf, "Patents Program Round Two");
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

		
		Job job_three = Job.getInstance(conf, "Patents Program Round Three");
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


	public static class PatentSum {

		private int patent_no;
		private int citations;

		public PatentSum ()
		{
			
		}

		public PatentSum (int patent_no, int citations)
		{
			this.patent_no = patent_no;
			this.citations = citations;
		}

		public int getPatentNo()
		{
			return patent_no;
		}

		public int getCitations()
		{
			return citations;
		}

		public void setPatentNo(int patent_no)
		{
			this.patent_no = patent_no;
		}

		public void setCitations(int citations)
		{
			this.citations = citations;
		}
		
	}

	
	public static class Map_One extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

			String[] vals = value.toString().split("[\\s]+");
			if( vals[0].equals(vals[1]) ) return;
			context.write(new IntWritable(Integer.parseInt(vals[0])), new Text(vals[1] + " " + new IntWritable(1)));
			context.write(new IntWritable(Integer.parseInt(vals[1])), new Text(vals[0] + " " + new IntWritable(0)));

		}

	}
	

	public static class Reduce_One extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			int sum = 0;
			for( Text val : values )
			{
				String[] split1 = val.toString().split("[\\s]+");
				IntWritable patent;
				if( split1[1].equals("1") )
				{
					patent = new IntWritable(Integer.parseInt(split1[1]));
					sum++;
					for( Text value : values )
					{
						String[] split2 = val.toString().split("[\\s]+");
						if( split2[1].equals("0") )
						{
							sum++;
						}
					}
					context.write(patent, new IntWritable(sum));
					sum = 0;
				}
			}
		}
	}

	public static void topPatents(IntWritable patent, int sum)
	{
		
		if( top_patents.size() < 10)
		{
			PatentSum newPatent = new PatentSum(patent.get(), sum);
			top_patents.add(newPatent);
		}
		
		/*
		for( int i = 0; i < NUM_TOP_PATENTS; i++ )
		{
			if( top_patents[i][1] < sum || top_patents[i][1] == 0 )
			{
				int temp_patent1 = top_patents[i][0];
				int temp_sum1 = top_patents[i][1];
				top_patents[i][0] = patent.get();
				top_patents[i][1] = sum;
				for( int j = i + 1; j < NUM_TOP_PATENTS; j++ )
				{
					int temp_patent2 = top_patents[j][0];
					int temp_sum2 = top_patents[j][1];
					top_patents[j][0] = temp_patent1;
					top_patents[j][1] = temp_sum1;
					temp_patent1 = temp_patent2;
					temp_sum1 = temp_sum2;
				}	
			}	
		}*/
	}




	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

			String[] vals = value.toString().split("[\\s]+");
			context.write(new IntWritable(Integer.parseInt(vals[0])), new IntWritable(Integer.parseInt(vals[2])));
	
		}

	}


	public static class Reduce_Two extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
			
			int sum = 0;
			for( IntWritable val : values )
			{
				sum += val.get();
			}

			if( sum != 0 )
			{
				//topPatents(key, sum);
				context.write(key, new IntWritable(sum));
			}

		}

	}


	public static class Map_Three extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
			String[] vals = value.toString().split("[\\s]+");
			
			if(top_patents.size() < 10)
			{
				top_patents.add(new PatentSum(Integer.parseInt(vals[0]), Integer.parseInt(vals[1])));
				//context.write(new IntWritable(Integer.parseInt(vals[0])), 
				//	      new IntWritable(Integer.parseInt(vals[1])));
				//return;
			}
			
			//if( top_patents.peek() != null && Integer.parseInt(vals[1]) >= top_patents.peek().getCitations() )
			int min = 0;
			int num = -1;
			for(int i = 0; i < top_patents.size(); i++)
			{
				if(top_patents.get(i).getCitations() < Integer.parseInt(vals[1]) 
					|| top_patents.get(i).getCitations() < top_patents.get(min).getCitations())
				{
					min = i;
					num = top_patents.get(min).getCitations();
				}
			}
			if( num != -1 )
			{	
				top_patents.remove(min);
				top_patents.add(new PatentSum(Integer.parseInt(vals[0]),
							      Integer.parseInt(vals[1])));
				context.write(new IntWritable(Integer.parseInt(vals[0])),
					      new IntWritable(Integer.parseInt(vals[1])));
			}
			
			/*
			if( Integer.parseInt(vals[1]) == top_patents[(NUM_TOP_PATENTS - 1)][1] )
			{
				context.write(new IntWritable(Integer.parseInt(vals[0])),
					      new IntWritable(Integer.parseInt(vals[1])));
			}
			*/

		}

	}


	
	public static class Reduce_Three extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

			/*Iterator it = top_patents.iterator();

			while( it.hasNext() )
			{
				PatentSum patent = (PatentSum) it.next();
				context.write(new IntWritable(patent.getPatentNo()), new IntWritable(patent.getCitations()));
			}*/

			PatentSum[] patents = top_patents.toArray(new PatentSum[top_patents.size()]);
			int[] sorted = new int[10];
			for(int i = 0; i < top_patents.size(); i++)
			{
				sorted[i] = patents[i].getCitations();
			}
			Arrays.sort(sorted);
			for( IntWritable val : values )
			{
				if(val.get() > sorted[0])
				{
					context.write(key, val);
				}
			}
			

			/*for(int i = 0; i < top_patents.size(); i++)
			{
				PatentSum patent = top_patents.get(i);
				context.write(new IntWritable(patent.getPatentNo()), new IntWritable(patent.getCitations()));
				top_patents.remove(i);
			}*/

		}

	}
}
