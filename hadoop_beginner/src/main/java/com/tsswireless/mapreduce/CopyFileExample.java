package com.tsswireless.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class CopyFileExample
{
	public static final String FILENAME_STRING = "/home/bhairav/tsswireless/dispatch/DISPATCH_BM.xml";
	
	public static void main(String[] args)
	{
		File dbFileConfiguration = new File(FILENAME_STRING);
		if (!dbFileConfiguration.exists())
		{
			System.out.println("File does not exist:" + FILENAME_STRING);
		}
		else
		{
			try
			{
				FileInputStream fi = new FileInputStream(dbFileConfiguration);
				byte[] buffer = new byte[1024];
				while (fi.read(buffer) > 0)
				{
					System.out.println(new String(buffer));
					buffer = new byte[1024];
				}
				
			}
			catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
}
