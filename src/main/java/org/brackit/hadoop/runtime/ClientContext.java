package org.brackit.hadoop.runtime;

import org.apache.hadoop.mapreduce.JobID;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;

public class ClientContext {

	private JobID[] jobIds;
	
	public ClientContext()
	{	
	}
	
	
	public JobID[] getJobIDs()
	{
		return jobIds;
	}

	public synchronized void attachJob(int seq, JobID jobId) throws QueryException
	{
		if (jobIds == null || jobIds.length <= seq) {
			throw new QueryException(ErrorCode.BIT_DYN_ABORTED_ERROR, 
					"ClientContext was not properly initialized"); 
		}
		jobIds[seq] = jobId;
	}


	public synchronized void init(int size)
	{
		jobIds = new JobID[size];
	}
}
