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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * @author metzler
 *
 */
public class SentenceWritable<T extends TokenWritable> implements Writable {

	private final TokenFactory<T> mFactory;
	private final List<T> mTokens = new ArrayList<T>();
	
	public SentenceWritable(TokenFactory<T> factory) {
		mFactory = factory;
		mTokens.clear();
	}

	public void setTokens(List<T> tokens) {
		mTokens.clear();
		mTokens.addAll(tokens);
	}
	
	public void addToken(T token) {
		mTokens.add(token);
	}

	public List<T> getTokens() {
		return mTokens;
	}

	public T getTokenAt(int index) {
		return mTokens.get(index);
		
	}

	public void clear() {
		mTokens.clear();
	}

	public int getNumTokens() {
		return mTokens.size();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int numTokens = in.readInt();
		for(int i = 0; i < numTokens; i++) {
			T token = mFactory.create();
			token.readFields(in);
			mTokens.add(token);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(mTokens.size());
		for(T token : mTokens) {
			token.write(out);
		}
	}
	
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("<sentence>");
		buf.append(TokenWritable.SEP);

		int i = 1;
		for(T token : mTokens) {
			buf.append(i++);
			buf.append(TokenWritable.SEP);
			buf.append(token.toString());
			buf.append(TokenWritable.SEP);
		}
		
		buf.append("</sentence>");
		
		return buf.toString();
	}

	public String toStringOfTokens() {
		StringBuffer buf = new StringBuffer();

		for(int i = 0; i < mTokens.size(); i++) {
			buf.append(mTokens.get(i).getToken());
			if(i != mTokens.size() - 1) {
				buf.append(' ');
			}
		}
		
		return buf.toString();
	}
}
