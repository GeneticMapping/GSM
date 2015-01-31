/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* “License”); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

//package org.apache.hadoop.mapreduce.lib.input;
package org.apache.hadoop.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class FastaInputFormat extends FileInputFormat<LongWritable, Text> {
public static final String LINES_PER_MAP =
“mapreduce.input.lineinputformat.linespermap”;

public RecordReader<LongWritable, Text> createRecordReader(
InputSplit genericSplit, TaskAttemptContext context)
throws IOException {
context.setStatus(genericSplit.toString());
return new FastaRecordReader();
}

/**
* Logically splits the set of input files for the job, splits N fasta sequences
* of the input as one split.
*
* @see FileInputFormat#getSplits(JobContext)
*/
public List<InputSplit> getSplits(JobContext job)
throws IOException {
List<InputSplit> splits = new ArrayList<InputSplit>();
int numLinesPerSplit = getNumLinesPerSplit(job);
for (FileStatus status : listStatus(job)) {
splits.addAll(getSplitsForFile(status,
job.getConfiguration(), numLinesPerSplit));
}
return splits;
}

public static List<FileSplit> getSplitsForFile(FileStatus status,
Configuration conf, int numLinesPerSplit) throws IOException {
List<FileSplit> splits = new ArrayList<FileSplit> ();
Path fileName = status.getPath();
if (status.isDir()) {
throw new IOException(“Not a file: ” + fileName);
}
FileSystem fs = fileName.getFileSystem(conf);
LineReader lr = null;
try {
FSDataInputStream in = fs.open(fileName);
lr = new LineReader(in, conf);
Text line = new Text();
int numLines = 0;
long begin = 0;
long length = 0;
int num = -1;
/**
while ((num = lr.readLine(line)) > 0) {
numLines++;
length += num;
if (numLines == numLinesPerSplit) {
// NLineInputFormat uses LineRecordReader, which always reads
// (and consumes) at least one character out of its upper split
// boundary. So to make sure that each mapper gets N lines, we
// move back the upper split limits of each split
// by one character here.
if (begin == 0) {
splits.add(new FileSplit(fileName, begin, length – 1,
new String[] {}));
} else {
splits.add(new FileSplit(fileName, begin – 1, length,
new String[] {}));
}
begin += length;
length = 0;
numLines = 0;
}
}
if (numLines != 0) {
splits.add(new FileSplit(fileName, begin, length, new String[]{}));
}
} finally {
if (lr != null) {
lr.close();
}
}
return splits;
*/
long record_length = 0;
int recordsRead = 0;
while ((num = lr.readLine(line)) > 0) {
if (line.toString().indexOf(“>”) >= 0){
recordsRead++;
}
if (recordsRead > numLinesPerSplit){
splits.add(new FileSplit(fileName, begin, record_length, new String[]{}));
begin = length;
record_length = 0;
recordsRead = 1;
}

length += num;
record_length += num;

}
splits.add(new FileSplit(fileName, begin, record_length, new String[]{}));

} finally {
if (lr != null) {
lr.close();
}
}
//LOG.info(splits.size() + “map tasks”);
return splits;
}

/**
* Set the number of lines per split
* @param job the job to modify
* @param numLines the number of lines per split
*/
public static void setNumLinesPerSplit(Job job, int numLines) {
job.getConfiguration().setInt(LINES_PER_MAP, numLines);
}

/**
* Get the number of lines per split
* @param job the job
* @return the number of lines per split
*/
public static int getNumLinesPerSplit(JobContext job) {
return job.getConfiguration().getInt(LINES_PER_MAP, 1);
}
}

/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* “License”); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

//package org.apache.hadoop.mapreduce.lib.input;
package org.apache.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
* Treats keys as offset in file and value as line.
*/
public class FastaRecordReader extends RecordReader<LongWritable, Text> {
private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

private CompressionCodecFactory compressionCodecs = null;
private long start;
private long pos;
private long end;
private LineReader in;
private int maxLineLength;
private LongWritable key = null;
private Text value = null;

public void initialize(InputSplit genericSplit,
TaskAttemptContext context) throws IOException {
FileSplit split = (FileSplit) genericSplit;
Configuration job = context.getConfiguration();
this.maxLineLength = job.getInt(“mapred.linerecordreader.maxlength”,
Integer.MAX_VALUE);
start = split.getStart();
end = start + split.getLength();
final Path file = split.getPath();
compressionCodecs = new CompressionCodecFactory(job);
final CompressionCodec codec = compressionCodecs.getCodec(file);

// open the file and seek to the start of the split
FileSystem fs = file.getFileSystem(job);
FSDataInputStream fileIn = fs.open(split.getPath());
boolean skipFirstLine = false;
if (codec != null) {
in = new LineReader(codec.createInputStream(fileIn), job);
end = Long.MAX_VALUE;
} else {
if (start != 0) {
skipFirstLine = true;
–start;
fileIn.seek(start);
}
in = new LineReader(fileIn, job);
}
if (skipFirstLine) { // skip first line and re-establish “start”.
start += in.readLine(new Text(), 0,
(int)Math.min((long)Integer.MAX_VALUE, end – start));
}
this.pos = start;
}

public boolean nextKeyValue() throws IOException {
if (key == null) {
key = new LongWritable();
}
key.set(pos);
if (value == null) {
value = new Text();
}
int newSize = 0;
/**
while (pos < end) {
newSize = in.readLine(value, maxLineLength,
Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
maxLineLength));
if (newSize == 0) {
break;
}
pos += newSize;
if (newSize < maxLineLength) {
break;
}

// line too long. try again
LOG.info(“Skipped line of size ” + newSize + ” at pos ” +
(pos – newSize));
}
if (newSize == 0) {
key = null;
value = null;
return false;
} else {
return true;
}
*/
while (pos < end) {
key.set(pos);

int newSize = in.readLine(value, maxLineLength,
Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
maxLineLength));
text = value.toString();
if (text.lastIndexOf(“>”) > 0){
break;
}else{
pos += newSize;
}

if (newSize == 0) {
break;
}
// line too long. try again
LOG.info(“Skipped line of size ” + newSize + ” at pos ” + (pos – newSize));
}
return true;
//return false;

}

@Override
public LongWritable getCurrentKey() {
return key;
}

@Override
public Text getCurrentValue() {
return value;
}

/**
* Get the progress within the split
*/
public float getProgress() {
if (start == end) {
return 0.0f;
} else {
return Math.min(1.0f, (pos – start) / (float)(end – start));
}
}

public synchronized void close() throws IOException {
if (in != null) {
in.close();
}
}
}