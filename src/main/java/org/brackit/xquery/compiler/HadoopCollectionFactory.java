package org.brackit.xquery.compiler;

import org.brackit.hadoop.io.HadoopCSVCollection;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.compiler.analyzer.CollectionFactory;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.type.ItemType;

public class HadoopCollectionFactory extends CollectionFactory {

	@Override
	public Collection<?> create(String name, String format, String location,
			String options, ItemType type) throws QueryException
	{
		if (format.equalsIgnoreCase("csv")) {
			return new HadoopCSVCollection(name, location, options, type);
		}
		return super.create(name, format, location, options, type);
	}

}
