package org.brackit.hadoop.collection;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.brackit.hadoop.job.XQueryJobConf;

public interface HadoopCollection {

	public void initHadoop(XQueryJobConf jobConf, Map<String, Serializable> properties) throws IOException;
	
}
