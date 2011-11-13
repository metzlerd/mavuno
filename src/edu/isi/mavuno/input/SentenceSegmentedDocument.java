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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.TokenFactory;
import edu.isi.mavuno.util.TokenWritable;

/**
 * @author metzler
 *
 */
public class SentenceSegmentedDocument<T extends TokenWritable> extends Indexable implements Passagifiable {

	// document id
	private String mDocId = null;

	// list of parsed sentences that make up the document
	private final List<SentenceWritable<T>> mSentences = new ArrayList<SentenceWritable<T>>();

	// token factory
	private final TokenFactory<T> mFactory;
	
	public SentenceSegmentedDocument(TokenFactory<T> factory) {
		mFactory = factory;
		mDocId = null;
		mSentences.clear();
	}

	public void setDocId(String id) {
		mDocId = id;
	}

	public void setSentences(List<SentenceWritable<T>> sentences) {
		mSentences.clear();
		mSentences.addAll(sentences);
	}

	public void addSentence(SentenceWritable<T> parsedSentence) {
		mSentences.add(parsedSentence);
	}

	public List<SentenceWritable<T>> getSentences() {
		return mSentences;
	}

	public SentenceWritable<T> getSentenceAt(int index) {
		return mSentences.get(index);
	}

	public void clear() {
		mDocId = null;
		mSentences.clear();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// read document id
		mDocId = in.readUTF();

		// read sentences
		mSentences.clear();		
		int numSentences = in.readInt();
		for(int i = 0; i < numSentences; i++) {
			SentenceWritable<T> s = new SentenceWritable<T>(mFactory);
			s.readFields(in);
			mSentences.add(s);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// write document id
		out.writeUTF(mDocId);

		// write sentences
		out.writeInt(mSentences.size());
		for(SentenceWritable<T> s : mSentences) {
			s.write(out);
		}
	}

	/* (non-Javadoc)
	 * @see edu.umd.cloud9.collection.Indexable#getContent()
	 */
	@Override
	public String getContent() {
		StringBuffer buf = new StringBuffer();

		for(SentenceWritable<T> s : mSentences) {
			buf.append(s.toStringOfTokens());
			buf.append(' ');
		}

		return buf.toString();
	}

	/* (non-Javadoc)
	 * @see edu.umd.cloud9.collection.Indexable#getDocid()
	 */
	@Override
	public String getDocid() {
		return mDocId;
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("<doc id=\">");
		buf.append(mDocId);
		buf.append("\">");
		buf.append(TokenWritable.SEP);
		
		for(SentenceWritable<T> s : mSentences) {
			buf.append(s.toString());
			buf.append(TokenWritable.SEP);
		}

		buf.append("</doc>");
		
		return buf.toString();
	}

	@Override
	public Indexable getPassage(int start, int length) {
		// make sure the request is feasible
		if(start + length >= mSentences.size()) {
			return null;
		}

		// create new document
		SentenceSegmentedDocument<T> passageDoc = new SentenceSegmentedDocument<T>(mFactory);

		// set document id
		passageDoc.setDocId(mDocId);

		// add left portion of passage
		for(int i = start - length; i < start; i++) {
			if(i >= 0) {
				passageDoc.addSentence(mSentences.get(i));
			}
		}

		// add middle portion of passage
		passageDoc.addSentence(mSentences.get(start));

		// add right portion of passage
		for(int i = start + 1; i <= start + length; i++) {
			passageDoc.addSentence(mSentences.get(i));
		}

		return passageDoc;
	}

}
