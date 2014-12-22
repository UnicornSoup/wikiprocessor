/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * sp: 
 * The original file is from Apache Mahout project. 
 * Customized this file to use the new map-reduce API
 */

package com.github.unicornsoup.wikiprocessor.main;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

/**
 * Reads records that are delimited by a specifc begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {
	static final Logger log = Logger.getLogger(XmlInputFormat.class);
	
  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";
  
 
  public RecordReader<LongWritable,Text> createRecordReader(InputSplit inputSplit,
		  TaskAttemptContext tac) {
    return new XmlRecordReader();
  }
  
  /**
   * XMLRecordReader class to read through a given xml document to output xml
   * blocks as records as specified by the start tag and end tag
   * 
   */
  public static class XmlRecordReader extends RecordReader<LongWritable,Text> {
    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable key = new LongWritable();
    private Text value = new Text();    
    
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
    	startTag = tac.getConfiguration().get(START_TAG_KEY).getBytes("utf-8");
    	log.debug("initialize: startTag=" + startTag);
    	endTag = tac.getConfiguration().get(END_TAG_KEY).getBytes("utf-8");
    	log.debug("initialize: endTag=" + endTag);
    	FileSplit split= (FileSplit) is;
    	start = split.getStart();
    	end = start + split.getLength();
    	log.debug("initialize: start:" + start + ":end:" + end);
    	Path file = split.getPath();
    	FileSystem fs = file.getFileSystem(tac.getConfiguration());
    	fsin = fs.open(split.getPath());
    	fsin.seek(start);
    	
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (fsin.getPos() < end) {
            if (readUntilMatch(startTag, false)) {
              try {
                buffer.write(startTag);
                if (readUntilMatch(endTag, true)) {
                  key.set(fsin.getPos());
                  value.set(buffer.getData(), 0, buffer.getLength());
                  return true;
                }
              } finally {
                buffer.reset();
              }
            }
          }
          return false;    	
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
    	return key;
    }
    
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
               return value;
    }

    
/*    @Override
    public long getPos() throws IOException {
      return fsin.getPos();
    }*/
    
    @Override
    public void close() throws IOException {
      fsin.close();
    }
    
    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }
    
    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        // if end of file, return false
        if (b == -1) return false;
        // save to buffer:
        if (withinBlock) buffer.write(b);
        // check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length) return true;
        } else i = 0;
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
      }
    }
  }
}