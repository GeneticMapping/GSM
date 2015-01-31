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

import java.io.*;
import java.util.*;
import java.net.URI;
import java.nio.charset.Charset;

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
	  String[] IDs = null;
	  int codonID = 1;
	  Text outputKey = null;
	  Text outputValue = null;
	  
	  Path[] localFiles;
	  
	  HashMap<String, ArrayList<String>> GenesIndex = null;
	  String currentGene = null;
	  ArrayList<String> currentRefSeq = null;
	  
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException{
		long currFileOffSet = 0;
		GenesIndex = new HashMap<String, ArrayList<String>>();
		outputKey = new Text();
		outputValue = new Text();

		localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		FileSystem fs = FileSystem.getLocal(context.getConfiguration());
		FSDataInputStream in = null;

		ArrayList<String> AuxRefSeq = new ArrayList<String>();
		byte[] buffer = new byte[3];
		int EOF = 2;
		String geneFilePath = null;
		String geneName = null;
		byte[] eofBuffer = new byte[2];

		for(int i=0; i<localFiles.length; i++) {
			geneFilePath = localFiles[i].getName();
			
			if(localFiles[i].toString().indexOf("generef/") != -1){
				geneName = geneFilePath.substring(0, geneFilePath.length()-9);
				
				try{
					in = fs.open(localFiles[i]);
					
					while(EOF == 2){
						in.read(currFileOffSet,buffer,0,3);
						AuxRefSeq.add(new String(buffer, "UTF-8"));
						
						currFileOffSet += 4;
						EOF = in.read(currFileOffSet, eofBuffer, 0, 2);
					}
				} catch(IOException e){
					throw new IOException("EOF: " + EOF + 
					"\neofBuffer: " + new String(eofBuffer, "UTF-8") + 
					"\ngeneFilePath: " + geneFilePath +
					"\ncurrFileOffSet: " + currFileOffSet +
					"\ngeneName: " + geneName, e);
				}finally {
					IOUtils.closeStream(in);
				}
				
				GenesIndex.put(geneName, AuxRefSeq);
				AuxRefSeq = new ArrayList<String>();
				EOF = 2;
				currFileOffSet = 0;
			}
		}
	  }
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {		
		codonID = 1;
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		if(tokenizer.hasMoreTokens()) { // gera um array com geneID e patientID
			IDs = tokenizer.nextToken().split(">")[1].split("_");
		}
		
		if(!IDs[0].equals(currentGene)) {
			currentRefSeq = GenesIndex.get(IDs[0]);
			currentGene = IDs[0];
			outputKey.set(IDs[0]);
		}
		
		String refSeq = null;
		String patSeq = null;
		
		while (tokenizer.hasMoreTokens()) {
			patSeq = tokenizer.nextToken();			
			refSeq = currentRefSeq.get(codonID - 1);
			
			if(!refSeq.equals(patSeq) && !patSeq.contains("nn")){ //&& !patSeq.contains("n") && !patSeq.contains("y") && !patSeq.contains("r") && !patSeq.contains("m") && !patSeq.contains("w") && !patSeq.contains("k") && !patSeq.contains("s")) {
				outputValue.set(IDs[1]+"_"+codonID+"_"+refSeq+"_"+patSeq);
				context.write(outputKey, outputValue);
			}
			
			++codonID;
		}
	  }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
	  Path[] localFiles;	  
	  HashMap<String, HashMap<String, String>> EnsemblDB = null;
	  HashMap<String, String> EnsemblDBHash = null;
	  ArrayList<String> currentGeneDB = null;
	  Text outputValue = null;
	  String codon_number = null;
	  String currValue = null;
	  Iterator<Text> valuesList = null;
	  
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException {
		EnsemblDB = new HashMap<String, HashMap<String, String>>();
		HashMap<String, String> AuxRefSeq = new HashMap<String, String>();
		outputValue = new Text();

		localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		String geneFilePath = null;
		String geneName = null;
		
		/////
		InputStream    fis;
		BufferedReader br;
		String         line;
		/////
		
		for(int i=0; i<localFiles.length; i++) {
			geneFilePath = localFiles[i].getName();
			
			if(localFiles[i].toString().indexOf("db/") != -1){
				geneName = geneFilePath.substring(0, geneFilePath.length()-4);
				
				/////
				fis = new FileInputStream(localFiles[i].toString());
				br = new BufferedReader(new InputStreamReader(fis));
				
				while ((line = br.readLine()) != null) {
					AuxRefSeq.put(line.split("\\s+")[0], line);
				}
				
				br.close();
				br = null;
				fis = null;
				/////
				
				EnsemblDB.put(geneName, AuxRefSeq);
				AuxRefSeq = new HashMap<String, String>();
			}
		}
	  }
	  
	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		
		EnsemblDBHash = EnsemblDB.get(key.toString());
		
		valuesList = values.iterator();
		
        while (valuesList.hasNext()) {
          currValue = valuesList.next().toString();
		  codon_number = currValue.split("_")[1];
		  
		  if(EnsemblDBHash.get(codon_number) != null){
			outputValue.set(currValue + " - DB data: " + EnsemblDBHash.get(codon_number));
			context.write(key, outputValue);
		  }
		  /*else{
			context.write(key, new Text(currValue.toString() + " - this mutation was not found in the current DataBase."));
		  }*/
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
	  
	  DistributedCache.addCacheFile(new Path("/user/brfilho/db/ABCB11.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/db/ABCB4.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/db/ATP8B1.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/db/JAG1.txt").toUri(), job.getConfiguration());
	  DistributedCache.addCacheFile(new Path("/user/brfilho/db/SERPINA1.txt").toUri(), job.getConfiguration());
	  
	  job.setJarByClass(DiseaseApplication_format_4.class);
	  job.setMapperClass(Map.class);
	  job.setCombinerClass(Reduce.class);
	  //job.setReducerClass(Reduce.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  //job.setNumReduceTasks(5);
	
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	  System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}