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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.JSONException;

/**
 * @author metzler
 */
public class TwitterInputFormat extends IndexableFileInputFormat<LongWritable, TwitterDocument> {

	@Override
	public RecordReader<LongWritable, TwitterDocument> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		return new TwitterRecordReader();
	}

	public static class TwitterRecordReader extends RecordReader<LongWritable, TwitterDocument> {

		private final LongWritable mCurKey = new LongWritable();
		private final TwitterDocument mCurValue = new TwitterDocument();
		
		private LineRecordReader mReader;

		public TwitterRecordReader() {
			mReader = new LineRecordReader();
		}

		@Override
		public void close() throws IOException {
			mReader.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return mCurKey;
		}

		@Override
		public TwitterDocument getCurrentValue() throws IOException, InterruptedException {
			return mCurValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return mReader.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			mReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (mReader.nextKeyValue() == false) {
				return false;
			}
			
			// set key
			mCurKey.set(mReader.getCurrentKey().get());
			
			// convert json input to twitter document format
			try {
				mCurValue.set(mReader.getCurrentValue());
			}
			catch(JSONException e) { // in case the json doesn't validate
				return false;
			}
			
			return true;
		}
	}
}
