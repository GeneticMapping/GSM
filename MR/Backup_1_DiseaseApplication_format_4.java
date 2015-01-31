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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class DiseaseApplication_format_4 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	  private JobConf conf;
	  private String word = null;
	  private String[] IDs = null;
	  private int codonID = 1;
	  private long currFileOffSet = 0;
	  
	  //private Path[] localArchives;
	  
	  @Override
	  public void configure(JobConf conf) {
	  	this.conf = conf;
		//localArchives = DistributedCache.getLocalCacheArchives(conf);
		//Utils.findNonClassPathArchive(IDs[0] + "_GENE.txt", conf);
	  }
	  
	  /*@Override
	  public void setup(Context context) throws IOException, InterruptedException{
	  	this.word = null;
	    this.IDs = null;
	    this.codonID = 1;
	    this.currFileOffSet = 0;
	  }*/
	  
	  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {		
		this.codonID = 1;
	    this.currFileOffSet = 0;
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		if(tokenizer.hasMoreTokens()) { // gera um array com geneID e patientID
			IDs = tokenizer.nextToken().split(">")[1].split("_");
		}
		
		String filepath = "hdfs://10.1.1.1/user/brfilho/generef/" + IDs[0] + "_GENE.txt";
		FileSystem fs = FileSystem.get(URI.create(filepath), this.conf);
		FSDataInputStream in = null;
		String refSeq = null;
		int bytes_read = 0;
		
		try {
			byte[] buffer = new byte[3];
			in = fs.open(new Path(filepath));
			
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();
				
				bytes_read = in.read(currFileOffSet,buffer,0,3);
				//if(bytes_read != 3) {break;}
				
				refSeq = new String(buffer, "UTF-8");
				
				if(!refSeq.equals(word) && !word.contains("n") && !word.contains("y") && !word.contains("r") && !word.contains("m") && !word.contains("w") && !word.contains("k") && !word.contains("s")) {
					output.collect(new Text(IDs[0]), new Text(IDs[1]+"_"+codonID+"_"+refSeq+"_"+word));
				}
				
				currFileOffSet += 4;
				++codonID;
			}
		} finally {
			IOUtils.closeStream(in);
		}
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {

        Text value = null;
        while (values.hasNext()) {
          value = values.next();
		  output.collect(key, new Text(value));
        }
      }
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(DiseaseApplication_format_4.class);
      conf.setJobName("DiseaseApplication_format_4");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	  
	  /*DistributedCache.addCacheArchive(new URI("/generef/ABCB11_GENE.txt", conf);
	  DistributedCache.addCacheArchive(new URI("/generef/ABCB4_GENE.txt", conf);
	  DistributedCache.addCacheArchive(new URI("/generef/ATP8B1_GENE.txt", conf);
	  DistributedCache.addCacheArchive(new URI("/generef/JAG1_GENE.txt", conf);
	  DistributedCache.addCacheArchive(new URI("/generef/SERPINA1_GENE.txt", conf);*/

      JobClient.runJob(conf);
    }
}