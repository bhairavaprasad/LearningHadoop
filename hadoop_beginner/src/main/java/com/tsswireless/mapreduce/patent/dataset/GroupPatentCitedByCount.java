package com.tsswireless.mapreduce.patent.dataset;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GroupPatentCitedByCount extends Configured implements Tool
{
			
	public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException
		{						
			StringTokenizer tokenizer = new StringTokenizer(values.next().toString(), ",");									
			output.collect(key, new IntWritable(tokenizer.countTokens()));
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
		jobConf.setMapperClass(IdentityMapper.class);		
		jobConf.setReducerClass(ReduceClass.class);
				
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
				
		FileInputFormat.addInputPath(jobConf, inPath);
		FileOutputFormat.setOutputPath(jobConf, outPath);
		
		JobClient.runJob(jobConf);
		
		return 0;				
	}
	
	public static void main(String[] args)
		throws Exception
	{
		int result = ToolRunner.run(new Configuration(), new GroupPatentCitedByCount(), args);
		System.exit(result);
	}
	

}
