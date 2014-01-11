package com.tsswireless.mapreduce.logdataset;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogDataSet extends Configured implements Tool
{
	
	public static class LogMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
		{
			String line = value.toString();
			String[] data = line.split("\\t");
			output.collect(new Text(data[5] + "_" + data[4]), value);
		}
	}
	
	public static class LogReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
		{
			while (values.hasNext())
			{
				output.collect(key, values.next());
			}
		}
		
	}
	
	public static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text>
	{
		
		@Override
		protected String generateFileNameForKeyValue(Text key, Text value, String name)
		{
			String sKey = key.toString();
			String[] splits = sKey.split("_");
			return splits[0] + File.separator + splits[1] + File.separator + key.toString();
		}
		
	}
	
	public static void main(String[] args)
		throws Exception
	{
		int result = ToolRunner.run(new Configuration(), new LogDataSet(), args);
		System.exit(result);
		
		/*
		 * String line =
		 * "612.57.72.653	03/Jun/2012:09:12:23 -0500	03	Jun	06	2012	09	12	23	-0500	GET	/product/product2	200	0	/product/product2	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30)"
		 * ; String[] data = line.split("\\t"); for(String str: data) {
		 * System.out.println(str); }
		 */
	}
	
	@Override
	public int run(String[] args)
		throws Exception
	{
		Configuration conf = getConf();
		JobConf jobConf = new JobConf(conf, LogDataSet.class);
		
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		
		jobConf.setJobName("LogData Set Mapper");
		jobConf.setMapperClass(LogMapper.class);
		jobConf.setReducerClass(LogReducer.class);
		
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(MultiFileOutput.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(jobConf, inPath);
		FileOutputFormat.setOutputPath(jobConf, outPath);
		
		JobClient.runJob(jobConf);
		
		return 0;
	}
	
}
