package com.tsswireless.mapreduce.patent.dataset;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GroupPatentCitedByCount extends Configured implements Tool
{

	
	public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text>
	{
		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
		{
			output.collect(value, key);
		}
	}
	
	public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException
		{			
			int count = 0;
			while (values.hasNext())
			{				
				count++;
			}
			output.collect(key, new IntWritable(count));
		}
		
	}
	
	@Override
	public int run(String[] args)
		throws Exception
	{
		Configuration conf = getConf();
		JobConf jobConf = new JobConf(conf, GroupPatentCitedByCount.class);
		
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
						
		jobConf.setJobName("GroupPatentCitedByCount");
		jobConf.setMapperClass(MapClass.class);		
		jobConf.setReducerClass(ReduceClass.class);
				
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		jobConf.set("key.value.separator.in.input.line", ",");
		
		FileInputFormat.addInputPath(jobConf, inPath);
		FileOutputFormat.setOutputPath(jobConf, outPath);
		
		JobClient.runJob(jobConf);
		
		return 0;
		
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args)
		throws Exception
	{
		int result = ToolRunner.run(new Configuration(), new GroupPatentCitedByCount(), args);
		System.exit(result);
	}
	

}
