package org.brackit.xquery.compiler;

import org.apache.hadoop.conf.Configuration;
import org.brackit.hadoop.collection.HBaseCollection;
import org.brackit.hadoop.collection.HadoopCSVCollection;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.compiler.analyzer.CollectionFactory;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.type.ItemType;

public class HadoopCollectionFactory extends CollectionFactory {

	private final Configuration conf;
	
	public HadoopCollectionFactory(Configuration conf)
	{
		this.conf = conf;
	}

	@Override
	public Collection<?> create(String name, String format, String location,
			String options, ItemType type) throws QueryException
	{
		if (format.equalsIgnoreCase("csv")) {
			return new HadoopCSVCollection(name, location, options, type);
		}
		else if (format.equals("hbase")) {
			return new HBaseCollection(conf, name, location, options, type);
		}
		else if (format.equals("mongo")) {
			
		}
		else if (format.equals("txt")) {
			
		}
		return super.create(name, format, location, options, type);
	}

}
