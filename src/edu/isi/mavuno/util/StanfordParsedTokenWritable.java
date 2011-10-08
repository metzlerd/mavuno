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

package edu.isi.mavuno.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

/**
 * @author metzler
 *
 */
public class StanfordParsedTokenWritable extends TratzParsedTokenWritable {
		
	// coref id
	private final IntWritable mCorefId = new IntWritable();
	
	public StanfordParsedTokenWritable() {
		super();
		mCorefId.set(0);
	}
	
	public void setCorefId(int id) {
		mCorefId.set(id);
	}

	public int getCorefId() {
		return mCorefId.get();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		mCorefId.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		mCorefId.write(out);
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		
		buf.append(super.toString());
		buf.append(SEP);
		buf.append(mCorefId);
		
		return buf.toString();
	}	

	public static class ExtendedParsedTokenFactory implements TokenFactory<StanfordParsedTokenWritable> {
		@Override
		public StanfordParsedTokenWritable create() {
			return new StanfordParsedTokenWritable();
		}
	}
}
