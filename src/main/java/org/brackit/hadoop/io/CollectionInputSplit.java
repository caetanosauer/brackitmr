package org.brackit.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;

public class CollectionInputSplit extends InputSplit {

	private final InputSplit inputSplit;
	private final Class<? extends InputFormat<?, ?>> inputFormatClass;
	private final int astBranch;
	
	public CollectionInputSplit(InputSplit inputSplit, Class<? extends InputFormat<?,?>> class1,
			int astBranch)
	{
		this.inputSplit = inputSplit;
		this.inputFormatClass = class1;
		this.astBranch = astBranch;
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

	public int getAstBranch()
	{
		return astBranch;
	}		

}
