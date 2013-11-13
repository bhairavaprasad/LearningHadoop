package com.tsswireless.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestMapper extends MapReduceBase implements Mapper
{
	
	public void map(Object key, Object value, OutputCollector output, Reporter reporter)
		throws IOException
	{
		// TODO Auto-generated method stub
		
	}
	
}
