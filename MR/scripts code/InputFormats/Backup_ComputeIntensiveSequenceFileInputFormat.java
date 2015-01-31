package com.navteq.fr.mapReduce.InputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class ComputeIntensiveSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {
	
	private static final double SPLIT_SLOP = 1.1; // 10% slop
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

	/**
	* Generate the list of files and make them into FileSplits.
	*/ 
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// get servers in the cluster
		String[] servers = getActiveServersList(job);
		if(servers == null) return null;
		
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus>files = listStatus(job);
		int currentServer = 0;
		
		for (FileStatus file: files) {
			Path path = file.getPath();
			long length = file.getLen();
			if ((length != 0) && isSplitable(job, path)) { 
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);

				long bytesRemaining = length;
				while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
					splits.add(new FileSplit(path, length-bytesRemaining, splitSize, new String[] {servers[currentServer]}));
					currentServer = getNextServer(currentServer, servers.length);
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, new String[] {servers[currentServer]}));
					currentServer = getNextServer(currentServer, servers.length);
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, new String[] {servers[currentServer]}));
				currentServer = getNextServer(currentServer, servers.length);
			} else { 
				//Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}

		// Save the number of input files in the job-conf
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

		return splits;
	}

	private String[] getActiveServersList(JobContext context){
	
		String [] servers = null;
		try {
			JobClient jc = new JobClient((JobConf)context.getConfiguration()); 
			ClusterStatus status = jc.getClusterStatus(true);
			Collection<String> atc = status.getActiveTrackerNames();
			servers = new String[atc.size()];
			int s = 0;
			
			for(String serverInfo : atc){
				StringTokenizer st = new StringTokenizer(serverInfo, ":");
				String trackerName = st.nextToken();
				StringTokenizer st1 = new StringTokenizer(trackerName, "_");
				st1.nextToken();
				servers[s++] = st1.nextToken();
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		
		return servers;
	}

	private static int getNextServer(int current, int max){
	
		current++;
		if(current >= max) current = 0;		
		return current;
	}
}

// Optimized getSplits()

public List<InputSplit> getSplits(JobContext job) throws IOException { 
	// get splits
	List<InputSplit> originalSplits = super.getSplits(job);

	// Get active servers
	String[] servers = getActiveServersList(job);
	if(servers == null) return null;
	
	// reassign splits to active servers
	List<InputSplit> splits = new ArrayList<InputSplit>(originalSplits.size());
	int numSplits = originalSplits.size();
	int currentServer = 0;
	
	for(int i = 0; i < numSplits; i++, currentServer = getNextServer(currentServer,	servers.length)){
		String server = servers[currentServer]; // Current server
		boolean replaced = false;
		
		// For every remaining split
		for(InputSplit split : originalSplits){
			FileSplit fs = (FileSplit)split;
			
			// For every split location
			for(String l : fs.getLocations()){
				// If this split is local to the server
				if(l.equals(server)){
					// Fix split location
					splits.add(new FileSplit(fs.getPath(), fs.getStart(), fs.getLength(), new String[] {server}));
					originalSplits.remove(split);
					replaced = true;
					break;
				}
			}
			
			if(replaced) break;
		}
		
		// If no local splits are found for this server
		if(!replaced){
			// Assign first available split to it 
			FileSplit fs = (FileSplit)splits.get(0);
			splits.add(new FileSplit(fs.getPath(), fs.getStart(), fs.getLength(), new String[] {server}));
			originalSplits.remove(0);
		}
	}
	return splits;
}

/*

Although the code (Listing 1, Listing 2) calculates splits locality correctly, when we tried to run
the code on our Hadoop cluster, we saw that it was not even close to producing even distribution
between servers. The problem that we have observed is well described in [2], which also describes
a solution for this problem - delayed fair scheduler.

Assuming that the fair scheduler is already setup, the following block should be added to the
mapred-site.xml file in order to enable a delayed scheduler[3]:

<property>
        <name>mapred.fairscheduler.locality.delay</name>
        <value>360000000</value>
<property>

*/