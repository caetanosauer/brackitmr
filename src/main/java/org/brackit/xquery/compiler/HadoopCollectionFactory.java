package org.brackit.xquery.compiler;

import org.apache.hadoop.conf.Configuration;
import org.brackit.hadoop.collection.HadoopCSVCollection;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.compiler.analyzer.CollectionFactory;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.type.ItemType;

public class HadoopCollectionFactory extends CollectionFactory {

	private final Configuration conf;
	
	private static final boolean REPLICATE_TO_DISTR_CACHE =
			Cfg.asBool(XQueryJobConf.PROP_REPLICATE_TO_DISTR_CACHE, false);
	
	private static final long REPLICATE_THRESHOLD =
			Cfg.asLong(XQueryJobConf.PROP_REPLICATE_THRESHOLD, 1000000);
	
	public HadoopCollectionFactory(Configuration conf)
	{
		this.conf = conf;
	}

	protected boolean isSmall(String location)
	{
		if (!REPLICATE_TO_DISTR_CACHE) {
			return false;
		}
		
		/*
		 * Here comes the code to get size of HDFS file
		 * You need to first check if 'location' is indeed an hdfs file (hdfs://...)
		 */
		long size = Long.MAX_VALUE;
		
		return size < REPLICATE_THRESHOLD;
	}
	
	@Override
	public Collection<?> create(String name, String format, String location,
			String options, ItemType type) throws QueryException
	{
		boolean small = isSmall(location);
		
		if (format.equalsIgnoreCase("csv")) {
			if (small) {
				/*
				 * Implement a collection that reads CSV data from distr. cache and uncomment:
				 * Replication should happen in the constructor of DistrCacheCollection
				 */
				// return new DistrCacheCollection(name, location, options type);
			}
			return new HadoopCSVCollection(name, location, options, type);
		}
		else if (format.equals("hbase")) {
//			return new HBaseLocalCollection(name, location, options, type);
		}
		else if (format.equals("mongo")) {
			
		}
		else if (format.equals("txt")) {
			
		}
		return super.create(name, format, location, options, type);
	}

}
