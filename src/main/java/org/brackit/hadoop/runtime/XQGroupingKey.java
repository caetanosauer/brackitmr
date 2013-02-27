package org.brackit.hadoop.runtime;

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;

public class XQGroupingKey implements Comparable<XQGroupingKey> {

	protected final Atomic[] keys;
	protected final int[] indexes;
	
	public XQGroupingKey(Atomic[] keys, int[] indexes) throws QueryException
	{
		this.keys = keys;
		this.indexes = indexes;
	}
	
	public XQGroupingKey(Tuple tuple, boolean isJoin, int ... indexes) throws QueryException
	{
		this.keys = new Atomic[indexes.length];
		this.indexes = indexes;
		
		int j = 0;
		for (int i : indexes) {
			Sequence seq = tuple.get(i);
			Iter iter = seq.iterate();
			keys[j] = iter.next().atomize();
			
			if (iter.next() != null) {
				throw new QueryException(ErrorCode.BIT_DYN_ABORTED_ERROR, 
						"Grouping keys may only contain atomic values!");
			}
			
			j++;
			
			if (!isJoin || i != tuple.getSize() - 1) { // TODO join hack
				tuple.array()[i] = null;
			}
		}
	}
	
	public Atomic[] getKeys() {
		return keys;
	}
	
	public Atomic getKeyAt(int index) {
		return keys[index];
	}

//	public void readFields(DataInput in) throws IOException
//	{
//		int length = in.readByte();
//		keys = new Atomic[length];
//		indexes = new int[length];
//		for (int i = 0; i < length; i++) {
//			keys[i] = (Atomic) ItemSerializer.readItem(in, null);
//		}
//		for (int i = 0; i < length; i++) {
//			indexes[i] = in.readByte();
//		}
//	}
//
//	public void write(DataOutput out) throws IOException
//	{
//		out.writeByte(keys.length);
//		for (int i = 0; i < keys.length; i++) {
//			ItemSerializer.writeItem(out, keys[i]);
//		}
//		for (int i = 0; i < keys.length; i++) {
//			out.writeByte(indexes[i]);
//		}
//	}
	
	@Override
	public int hashCode()
	{
		// TODO: implement efficient type-dependent hash code
		int hash = 137;
		int multiplier = 13;
		for (int i = 0; i < keys.length; i++) {
			hash = hash * multiplier + keys[i].hashCode();
		}
		return hash;
	}
	
	// this ignores the last key, which is by assumption the tag in case of a join
	public int joinHashCode()
	{
		// TODO: implement efficient type-dependent hash code
		int hash = 137;
		int multiplier = 13;
		for (int i = 0; i < keys.length - 1; i++) {
			hash = hash * multiplier + keys[i].hashCode();
		}
		return hash;
	}

	public int compareTo(XQGroupingKey other)
	{
		for (int i = 0; i < keys.length; i++) {
			int cmp = keys[i].compareTo(other.keys[i]);
			if (cmp != 0) {
				return cmp;
			}
		}
		
		if (keys.length != other.keys.length) {
			return keys.length > other.keys.length ? 1 : -1;
		}
		
		return 0;
	}
	
	public int joinCompareTo(XQGroupingKey other)
	{
		for (int i = 0; i < keys.length - 1; i++) {
			int cmp = keys[i].atomicCmp(other.keys[i]);
			if (cmp != 0) {
				return cmp;
			}
		}
		
		if (keys.length != other.keys.length) {
			return keys.length > other.keys.length ? 1 : -1;
		}
		
		return 0;
	}
	
	public String toString()
	{
		if (keys == null) return "null";
		
		StringBuffer sb = new StringBuffer("[");
		for (int i = 0; i < keys.length; i++) {
			sb.append(keys[i].stringValue());
			if (i < keys.length - 1) sb.append("; ");
		}
		sb.append(']');
		
		return sb.toString();
	}

	public void rebuildTuple(Tuple tuple) throws QueryException
	{
		rebuildTuple(tuple, false);
	}
	
	public void rebuildTuple(Tuple tuple, boolean isJoin) throws QueryException
	{
		Sequence[] seqs = tuple.array();
		int max = 0;
		if (indexes != null) {
			max = indexes.length;
			if (isJoin) max--;
			for (int i = 0; i < max; i++) {
				seqs[indexes[i]] = keys[i];
			}
		}
		else {
			max = seqs.length;
			if (isJoin) max--;
			for (int i = 0, j = 0; i < max; i++) {
				if (seqs[i] == null) {
					seqs[i] = keys[j++];
				}
			}
		}
	}
	
	// TODO: equals

}
