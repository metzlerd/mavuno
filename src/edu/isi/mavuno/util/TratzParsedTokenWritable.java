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
import org.apache.hadoop.io.Text;

/**
 * @author metzler
 *
 */
public class TratzParsedTokenWritable extends TokenWritable {

	// character offset
	private final IntWritable mOffsetBegin = new IntWritable();
	private final IntWritable mOffsetEnd = new IntWritable();

	// token lemma
	private final Text mLemma = new Text();

	// token part of speech
	private final Text mPosTag = new Text();

	// chunk tag
	private final Text mChunkTag = new Text();
	
	// named entity tag
	private final Text mNETag = new Text();

	// dependency type
	private final Text mDependType = new Text();
	
	// dependency position
	private final IntWritable mDependIndex = new IntWritable();
		
	public TratzParsedTokenWritable() {
		super();
		mOffsetBegin.set(0);
		mOffsetEnd.set(0);
		mLemma.clear();
		mPosTag.clear();
		mChunkTag.clear();
		mNETag.clear();
		mDependType.clear();
		mDependIndex.set(0);
	}

	public void setCharOffset(int begin, int end) {
		mOffsetBegin.set(begin);
		mOffsetEnd.set(end);
	}

	public int getCharOffsetBegin() {
		return mOffsetBegin.get();
	}
	
	public int getCharOffsetEnd() {
		return mOffsetEnd.get();
	}

	public void setLemma(Text lemma) {
		safeSet(mLemma, lemma);
	}

	public void setLemma(String lemma) {
		safeSet(mLemma, lemma);
	}

	public Text getLemma() {
		return mLemma;
	}

	public void setPosTag(Text pos) {
		safeSet(mPosTag, pos);
	}

	public void setPosTag(String pos) {
		safeSet(mPosTag, pos);
	}

	public Text getPosTag() {
		return mPosTag;
	}

	public void setChunkTag(Text tag) {
		safeSet(mChunkTag, tag);
	}
	
	public void setChunkTag(String tag) {
		safeSet(mChunkTag, tag);
	}

	public Text getChunkTag() {
		return mChunkTag;
	}

	public void setNETag(Text tag) {
		safeSet(mNETag, tag);
	}

	public void setNETag(String tag) {
		safeSet(mNETag, tag);
	}

	public Text getNETag() {
		return mNETag;
	}
	
	public void setDepdentType(Text type) {
		safeSet(mDependType, type);
	}

	public void setDependType(String type) {
		safeSet(mDependType, type);
	}

	public Text getDependType() {
		return mDependType;
	}
	
	public void setDependIndex(int index) {
		mDependIndex.set(index);
	}

	public int getDependIndex() {
		return mDependIndex.get();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		mOffsetBegin.readFields(in);
		mOffsetEnd.readFields(in);
		mLemma.readFields(in);
		mPosTag.readFields(in);
		mChunkTag.readFields(in);
		mNETag.readFields(in);
		mDependType.readFields(in);
		mDependIndex.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		mOffsetBegin.write(out);
		mOffsetEnd.write(out);
		mLemma.write(out);
		mPosTag.write(out);
		mChunkTag.write(out);
		mNETag.write(out);
		mDependType.write(out);
		mDependIndex.write(out);
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		
		buf.append(super.toString());
		buf.append(SEP);
		buf.append(mOffsetBegin);
		buf.append(SEP);
		buf.append(mOffsetEnd);
		buf.append(SEP);
		buf.append(mLemma);
		buf.append(SEP);
		buf.append(mPosTag);
		buf.append(SEP);
		buf.append(mChunkTag);
		buf.append(SEP);
		buf.append(mNETag);
		buf.append(SEP);
		buf.append(mDependType);
		buf.append(SEP);
		buf.append(mDependIndex);
		
		return buf.toString();
	}
	
	public static class ParsedTokenFactory implements TokenFactory<TratzParsedTokenWritable> {
		@Override
		public TratzParsedTokenWritable create() {
			return new TratzParsedTokenWritable();
		}
	}
}
