package org.brackit.hadoop.collection;

import java.io.IOException;

import org.brackit.hadoop.job.XQueryJobConf;

public interface HadoopCollection {

	public void initHadoop(XQueryJobConf jobConf) throws IOException;
	
}
