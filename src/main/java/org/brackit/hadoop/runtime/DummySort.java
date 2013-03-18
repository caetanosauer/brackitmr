package org.brackit.hadoop.runtime;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progressable;

public class DummySort implements IndexedSorter {

//	private QuickSort qSort = new QuickSort();
	
	public DummySort()
	{
	}

	public void sort(IndexedSortable s, int l, int r)
	{
//		sort(s, l, r, null);
	}

	public void sort(IndexedSortable s, int l, int r, Progressable rep)
	{
//		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
//		for (StackTraceElement ste : stackTrace) {
//			System.out.println(ste.getClassName() + "." + ste.getMethodName());
//			if (ste.getClassName().equals("org.apache.hadoop.mapred.MapTask$MapOutputBuffer")
//					&& ste.getMethodName().equals("sortAndSpill")) {
//				System.out.println("Skipping quick sort");
//				return;
//			}
//		}
//		qSort.sort(s, l, r, rep);
	}

}
