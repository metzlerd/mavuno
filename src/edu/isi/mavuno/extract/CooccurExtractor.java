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

package edu.isi.mavuno.extract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.input.SimpleTokenizer;
import edu.isi.mavuno.input.Tokenizer;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class CooccurExtractor extends Extractor {

	// tokenizer
	private Tokenizer mTokenizer = null;

	// maximum n-gram size
	private int mMaxGramSize;

	private final Text mPattern = new Text();

	private Iterator<ContextPatternWritable> mPairsIter = null;
	private final List<ContextPatternWritable> mPairs = new ArrayList<ContextPatternWritable>();

	/* (non-Javadoc)
	 * @see edu.isi.mavuno.extract.Extractor#initialize(java.lang.String, org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] args = argSpec.split(":");

		// max ngram size
		mMaxGramSize = Integer.parseInt(args[0]);

		// set up tokenizer
		mTokenizer = new SimpleTokenizer();
		mTokenizer.configure(conf);
	}

	/* (non-Javadoc)
	 * @see edu.isi.mavuno.extract.Extractor#setDocument(edu.umd.cloud9.collection.Indexable)
	 */
	@Override
	public void setDocument(Indexable doc) {
		loadPairs(getTerms(doc), doc.getDocid());
		mPairsIter = mPairs.iterator();
	}

	/* (non-Javadoc)
	 * @see edu.isi.mavuno.extract.Extractor#getNextPair(edu.isi.mavuno.util.ContextPatternWritable)
	 */
	@Override
	public boolean getNextPair(ContextPatternWritable pair) {
		if(mPairsIter != null && mPairsIter.hasNext()) {
			ContextPatternWritable c = mPairsIter.next();
			pair.setContext(c.getContext());
			pair.setPattern(c.getPattern());
			return true;
		}

		return false;
	}

	protected Text [] getTerms(Indexable doc) {
		String text = doc.getContent();
		return mTokenizer.processContent(text);
	}

	protected boolean getPattern(Text pattern, Text [] terms, int start, int len) {
		if(start < 0 || start + len > terms.length) {
			return false;
		}

		pattern.clear();
		for(int i = start; i < start + len; i++) {
			pattern.append(terms[i].getBytes(), 0, terms[i].getLength());
			if(i != start + len - 1) {
				pattern.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
			}
		}

		return true;
	}

	private void loadPairs(Text [] terms, String id) {
		mPairs.clear();

		ContextPatternWritable c;

		for(int i = 0; i < terms.length; i++) {
			for(int gramSize = 1; gramSize <= mMaxGramSize; gramSize++) {
				if(!getPattern(mPattern, terms, i, gramSize)) {
					continue;
				}

				c = new ContextPatternWritable();
				c.setContext(id);
				c.setPattern(mPattern);
				mPairs.add(c);
			}
		}
	}
}
