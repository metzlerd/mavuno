/*
 * Mavuno: A Hadoop-Based Text Mining Toolkit
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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TextFileInputFormat extends FileInputFormat<LongWritable, TextDocument> {

	@Override
	public RecordReader<LongWritable, TextDocument> createRecordReader(InputSplit split, TaskAttemptContext context) {
		Configuration conf = context.getConfiguration();
		try {
			return new TextFileRecordReader((FileSplit)split, conf);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	
	public static class TextFileRecordReader extends RecordReader<LongWritable, TextDocument> {
		
		private final LongWritable mCurKey = new LongWritable();
		private final TextDocument mCurValue = new TextDocument();

		private long mStart;
		private long mEnd;
		private long mPos;
		private long mRecordStartPos;

		private final String mFileName;

		private DataInputStream mInput = null;
		private final DataOutputBuffer mBuffer = new DataOutputBuffer();

		public TextFileRecordReader(FileSplit split, Configuration conf) throws IOException {
			mStart = split.getStart();

			Path file = split.getPath();
			mFileName = file.getName();

			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
			CompressionCodec codec = compressionCodecs.getCodec(file);

			FileSystem fs = file.getFileSystem(conf);

			if (codec != null) {
				mInput = new DataInputStream(codec.createInputStream(fs.open(file)));				
				mEnd = Long.MAX_VALUE;
			} else {
				FSDataInputStream fileIn = fs.open(file);

				fileIn.seek(mStart);
				mInput = fileIn;

				mEnd = mStart + split.getLength();
			}

			mRecordStartPos = mStart;

			// Because input streams of gzipped files are not seekable
			// (specifically, do not support getPos), we need to keep
			// track of bytes consumed ourselves.
			mPos = mStart;
		}

		@Override
		public void close() throws IOException {
			if(mInput != null) {
				mInput.close();
			}
		}

		@Override
		public float getProgress() throws IOException {
			return ((float) (mPos - mStart)) / ((float) (mEnd - mStart));
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return mCurKey;
		}

		@Override
		public TextDocument getCurrentValue() throws IOException, InterruptedException {
			return mCurValue;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			mBuffer.reset();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(mInput == null) {
				return false;
			}
			
			while(mPos < mEnd) {
				int b = mInput.read();

				// increment position (bytes consumed)
				mPos++;

				// mEnd of file
				if (b == -1) {
					break;
				}

				mBuffer.write(b);
			}

			mCurKey.set(mRecordStartPos);
			mCurValue.set(mFileName, new String(mBuffer.getData()));
			
			mInput.close();
			mInput = null;
			
			return true;
		}
	}
}
