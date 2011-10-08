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

/**
 * @author metzler
 *
 */
public class LineInputFormat extends IndexableFileInputFormat<LongWritable, TextDocument> {

	/**
	 * Returns a <code>RecordReader</code> for this <code>InputFormat</code>.
	 */
	@Override
	public RecordReader<LongWritable, TextDocument> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		return new TextDocumentRecordReader(inputSplit, context);
	}

	/**
	 * Hadoop <code>RecordReader</code> for reading text documents (one document per line).
	 */
	public static class TextDocumentRecordReader extends RecordReader<LongWritable, TextDocument> {

		private final LongWritable mCurKey = new LongWritable();
		private final TextDocument mCurValue = new TextDocument();
		
		private LineRecordReader mReader;

		/**
		 * Creates a <code>TextDocumentRecordReader</code>.
		 */
		public TextDocumentRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
			mReader = new LineRecordReader();
			mReader.initialize(inputSplit, context);
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
		public TextDocument getCurrentValue() throws IOException, InterruptedException {
			return mCurValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return mReader.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (mReader.nextKeyValue() == false) {
				return false;
			}
			mCurKey.set(mReader.getCurrentKey().get());

			String str = mReader.getCurrentValue().toString();

			String docid = null;
			String text = null;

			String [] cols = str.split("\t");
			if(cols.length == 2) { // docid [tab] text formatted lines
				docid = cols[0];
				text = str.substring(str.indexOf('\t') + 1);
			}
			else { // all other lines are assigned the reader key as the docid
				docid = mReader.getCurrentKey().toString();
				text = str;
			}
			
			mCurValue.set(docid, text);
			return true;
		}
	}
}
