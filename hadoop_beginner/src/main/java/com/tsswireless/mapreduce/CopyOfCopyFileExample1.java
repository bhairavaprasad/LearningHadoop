package com.tsswireless.mapreduce;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class CopyOfCopyFileExample1
{
	public static final String FILENAME_STRING = "/home/bhairav/tsswireless/dispatch/DISPATCH_BM.xml";
	
	public static void main(String[] args)
	{
		try
		{
			RandomAccessFile dbFileConfiguration = new RandomAccessFile(FILENAME_STRING, "r");
			FileChannel inChannel = dbFileConfiguration.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(1024);
			while (inChannel.read(buffer) > 0)
			{
				buffer.flip();
				for (int i = 0; i < buffer.limit(); i++)
				{
					System.out.print((char) buffer.get());
				}
				buffer.clear();
			}
			inChannel.close();
			dbFileConfiguration.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
