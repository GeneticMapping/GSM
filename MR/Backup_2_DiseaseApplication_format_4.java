/*
format number - 4
patient data - 1 gene per line (already divided in 3 letters block - amino acids)

TextInputFormat - key (byteoffset) - value (line content)
*/
/*
Mapper:
	input_key (Text)    - geneID_patientID (first string after '>')
	input_value (Text)  - the rest of the line
	
	function            - split key (ex.: geneID_patientID)
					      read gene file using geneID
					      split value
					      start comparison between gene file and values
				   
	output_key (Text)   - geneID
	output_value (Text) - list(patientID_codonID_RefSeq_PatientSeq)
*/
/*
Reducer:
	input_key (Text)    - geneID
	input_value (Text)  - list(patientID_codonID_RefSeq_PatientSeq)
	
	function            - search Ensembl DB
				             if change exist -> create its SeqChangeData
				             else -> SeqChangeData = non existent change in DB
					  
	output_key (Text)   - geneID
	output_value (Text) - list(patientID_codonID_RefSeq_PatientSeq_SeqChangeData)
*/

package org.HCPAtools.Hadoop;

import java.io.IOException;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;

public class DiseaseApplication_format_4 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	  private String word = null;
	  private String[] IDs = null;
	  private int codonID = 1;
	  private long currFileOffSet = 0;
	  
	  private Path[] localFiles;
	  //private URI[] localArchivesURI;
	  
	  /*@Override
	  public void configure(JobConf conf) {
	  	this.conf = conf;
		//URI = DistributedCache.getCacheArchives();
	  }*/
	  
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException{
	  	this.word = null;
	    this.IDs = null;
	    this.codonID = 1;
	    this.currFileOffSet = 0;
		
		this.localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	  }
	  
	  public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {		
		this.codonID = 1;
	    this.currFileOffSet = 0;
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		if(tokenizer.hasMoreTokens()) { // gera um array com geneID e patientID
			IDs = tokenizer.nextToken().split(">")[1].split("_");
		}
		
		int x = 0;
		String filename = IDs[0];
		for(int i=0; i<localFiles.length; i++) {
			if (localFiles[i].getName().contains(filename)) {
				x = i;
				break;
			}
		}
		
		FileSystem fs = FileSystem.getLocal(context.getConfiguration());
		FSDataInputStream in = null;
		String refSeq = null;
		
		try {
			byte[] buffer = new byte[3];
			in = fs.open(localFiles[x]);
			
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();
				
				in.read(currFileOffSet,buffer,0,3);
				
				refSeq = new String(buffer, "UTF-8");
				
				if(!refSeq.equals(word) && !word.contains("n") && !word.contains("y") && !word.contains("r") && !word.contains("m") && !word.contains("w") && !word.contains("k") && !word.contains("s")) {
					context.write(new Text(IDs[0]), new Text(IDs[1]+"_"+codonID+"_"+refSeq+"_"+word));
				}
				
				currFileOffSet += 4;
				++codonID;
			}
		} finally {
			IOUtils.closeStream(in);
		}
      }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, Context context)
		throws IOException, InterruptedException {

        Text value = null;
        while (values.hasNext()) {
          value = values.next();
		  context.write(key, new Text(value));
        }
      }
    }

    public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  
	  Job job = new Job(conf, "DiseaseApplication_format_4");
	  
	  DistributedCache.addCacheFile(new Path("/user/brfilho/generef/ABCB11_GENE.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/generef/ABCB4_GENE.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/generef/ATP8B1_GENE.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/generef/JAG1_GENE.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/generef/SERPINA1_GENE.txt").toUri(), job.getConfiguration());
	  
	  
	  job.setJarByClass(DiseaseApplication_format_4.class);
	  job.setMapperClass(Map.class);
	  job.setReducerClass(Reduce.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
      //JobClient.runJob(conf);
    }
}