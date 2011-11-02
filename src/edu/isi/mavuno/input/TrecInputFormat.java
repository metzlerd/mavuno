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


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.isi.mavuno.input.TrecDocument;
import edu.isi.mavuno.input.XMLInputFormat.XMLRecordReader;

/**
 * Hadoop <code>InputFormat</code> for processing the TREC collection.
 * 
 * @author Jimmy Lin
 * @author metzler
 */
public class TrecInputFormat extends IndexableFileInputFormat<LongWritable, TrecDocument> {

	/**
	 * Returns a <code>RecordReader</code> for this <code>InputFormat</code>.
	 */
	@Override
	public RecordReader<LongWritable, TrecDocument> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		return new TrecDocumentRecordReader((FileSplit) inputSplit, conf);
	}

	/**
	 * Hadoop <code>RecordReader</code> for reading TREC-formatted documents.
	 */
	public static class TrecDocumentRecordReader extends RecordReader<LongWritable, TrecDocument> {

		private final LongWritable mCurKey = new LongWritable();
		private final TrecDocument mCurValue = new TrecDocument();
		
		private XMLRecordReader mReader;

		/**
		 * Creates a <code>TrecDocumentRecordReader</code>.
		 */
		public TrecDocumentRecordReader(FileSplit split, Configuration conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, TrecDocument.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, TrecDocument.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);
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
		public TrecDocument getCurrentValue() throws IOException, InterruptedException {
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
			TrecDocument.readDocument(mCurValue, mReader.getCurrentValue().toString());
			return true;
		}
	}
}
