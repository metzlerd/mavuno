/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.isi.mavuno.input;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

// solution for reading XML files, posted to the Hadoop users mailing list
// Re: map/reduce function on xml string - Colin Evans-2 Mar 04, 2008; 02:27pm
public class XMLInputFormat extends TextInputFormat {
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();
		try {
			return new XMLRecordReader((FileSplit)split, conf);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static class XMLRecordReader extends RecordReader<LongWritable, Text> {
		private final LongWritable mCurKey = new LongWritable();
		private final Text mCurValue = new Text();
		
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private long pos;
		private DataInputStream fsin = null;
		private final DataOutputBuffer buffer = new DataOutputBuffer();

		private long recordStartPos;

		public XMLRecordReader(FileSplit split, Configuration conf) throws IOException {
			if (conf.get(START_TAG_KEY) == null || conf.get(END_TAG_KEY) == null)
				throw new RuntimeException("Error! XML start and end tags unspecified!");

			startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
			endTag = conf.get(END_TAG_KEY).getBytes("utf-8");

			start = split.getStart();
			Path file = split.getPath();

			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
			CompressionCodec codec = compressionCodecs.getCodec(file);
			
			FileSystem fs = file.getFileSystem(conf);

			if (codec != null) {
				fsin = new DataInputStream(codec.createInputStream(fs.open(file)));				
				end = Long.MAX_VALUE;
			} else {
				FSDataInputStream fileIn = fs.open(file);

				fileIn.seek(start);
				fsin = fileIn;
				
				end = start + split.getLength();
			}

			recordStartPos = start;

			// Because input streams of gzipped files are not seekable
			// (specifically, do not support getPos), we need to keep
			// track of bytes consumed ourselves.
			pos = start;
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public float getProgress() throws IOException {
			return ((float) (pos - start)) / ((float) (end - start));
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// increment position (bytes consumed)
				pos++;
				
				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && pos >= end)
					return false;
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return mCurKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return mCurValue;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (pos < end) {
				if (readUntilMatch(startTag, false)) {
					recordStartPos = pos - startTag.length;
					
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							mCurKey.set(recordStartPos);
							mCurValue.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}
	}
}
