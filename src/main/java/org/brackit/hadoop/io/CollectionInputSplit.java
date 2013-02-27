package org.brackit.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;

public class CollectionInputSplit extends InputSplit {

	private final InputSplit inputSplit;
	private final Class<? extends InputFormat<?, ?>> inputFormatClass;
	
	
	public CollectionInputSplit(InputSplit inputSplit, Class<? extends InputFormat<?, ?>> inputFormatClass)
	{
		this.inputSplit = inputSplit;
		this.inputFormatClass = inputFormatClass;
	}

	@Override
	public long getLength() throws IOException, InterruptedException
	{
		return inputSplit.getLength();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException
	{
		return inputSplit.getLocations();
	}

	public Class<? extends InputFormat<?, ?>> getInputFormatClass()
	{
		return inputFormatClass;
	}
	
	

}
