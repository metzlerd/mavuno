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
public class NGramExtractor extends Extractor {

	// temporary left context, pattern, right context storage
	private final Text mLeftContext = new Text();
	private final Text mPattern = new Text();
	private final Text mRightContext = new Text();

	// tokenizer
	private Tokenizer mTokenizer = null;

	// extractor parameters
	private int mMinContextSize;
	private int mMaxContextSize;
	private int mMaxPatternSize;
	private int mMaxSkipSize;

	private boolean mOrContextStyle = false;
	private boolean mLeftOnlyContextStyle = false;
	private boolean mRightOnlyContextStyle = false;

	// document terms
	private Text [] mTerms = null;

	// previous configuration
	private int mPrevPos;
	private int mPrevPatternSize;
	private int mPrevLeftContextSize;
	private int mPrevRightContextSize;
	private int mPrevLeftSkipSize;
	private int mPrevRightSkipSize;

	// current configuration
	private int mCurPos;
	private int mCurPatternSize;
	private int mCurLeftContextSize;
	private int mCurRightContextSize;
	private int mCurLeftSkipSize;
	private int mCurRightSkipSize;

	public NGramExtractor() {
		mTerms = null;
	}

	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] args = argSpec.split(":");
		mMinContextSize = Integer.parseInt(args[0]);
		mMaxContextSize = Integer.parseInt(args[1]);
		mMaxPatternSize = Integer.parseInt(args[2]);
		mMaxSkipSize = Integer.parseInt(args[3]);

		String contextStyle = args[4].toLowerCase();

		mOrContextStyle = true;
		mLeftOnlyContextStyle = false;
		mRightOnlyContextStyle = false;

		if(contextStyle.equals("l*r")) {
			mOrContextStyle = false;
		}
		else if(contextStyle.equals("l+r")) {
			mOrContextStyle = true;
		}
		else if(contextStyle.equals("l")) {
			mLeftOnlyContextStyle = true;
		}
		else if(contextStyle.equals("r")) {
			mRightOnlyContextStyle = true;
		}
		else {
			throw new RuntimeException("Unrecognized NGramExtractor context style = " + contextStyle);
		}

		// set up tokenizer
		mTokenizer = new SimpleTokenizer();
		mTokenizer.configure(conf);
	}

	@Override
	public void setDocument(final Indexable doc) {
		mTerms = getTerms(doc);

		mPrevPos = -1;
		mPrevPatternSize = -1;
		mPrevLeftSkipSize = -1;
		mPrevLeftContextSize = -1;
		mPrevRightSkipSize = -1;
		mPrevRightContextSize = -1;		

		mCurPos = 0;
		mCurPatternSize = 1;
		mCurLeftSkipSize = 0;
		mCurLeftContextSize = mMinContextSize;
		mCurRightSkipSize = 0;
		mCurRightContextSize = mMinContextSize;
	}

	@Override
	public boolean getNextPair(final ContextPatternWritable pair) {
		for(; mCurPos < mTerms.length; mCurPos++) {
			for(; mCurPatternSize <= mMaxPatternSize; mCurPatternSize++) {
				if(!getPattern(mPattern, mTerms, mCurPos, mCurPatternSize)) {
					continue;
				}

				for(; mCurLeftSkipSize <= mMaxSkipSize; mCurLeftSkipSize++) {
					boolean validLeftContext = false;
					for(; mCurLeftContextSize <= mMaxContextSize; mCurLeftContextSize++) {
						validLeftContext = getLeftContext(mLeftContext, mTerms, mCurPos-mCurLeftSkipSize-mCurLeftContextSize, mCurLeftContextSize);
						if(!validLeftContext && !mOrContextStyle) {
							continue;
						}

						if(mOrContextStyle && !mRightOnlyContextStyle && validLeftContext) {
							if(!sameAsLast()) {
								pair.setPattern(mPattern);
								pair.setContext(MavunoUtils.createContext(mLeftContext,ContextPatternWritable.ASTERISK));
								saveConfig();
								return true;
							}
						}

						if(mOrContextStyle && (mLeftOnlyContextStyle || mCurLeftContextSize > mMinContextSize)) {
							continue;
						}

						for(; mCurRightSkipSize <= mMaxSkipSize; mCurRightSkipSize++) {
							for(; mCurRightContextSize <= mMaxContextSize; mCurRightContextSize++) {
								if(!getRightContext(mRightContext, mTerms, mCurPos+mCurRightSkipSize+mCurPatternSize, mCurRightContextSize)) {
									continue;
								}

								if(mOrContextStyle) {
									if(!sameAsLast()) {
										pair.setPattern(mPattern);
										pair.setContext(MavunoUtils.createContext(ContextPatternWritable.ASTERISK, mRightContext));
										saveConfig();
										return true;
									}
								}
								else {
									if(!sameAsLast()) {
										pair.setPattern(mPattern);
										pair.setContext(MavunoUtils.createContext(mLeftContext, mRightContext));
										saveConfig();
										return true;
									}
								}
							}
							mCurRightContextSize = mMinContextSize;
						}
						mCurRightSkipSize = 0;
					}
					mCurLeftContextSize = mMinContextSize;

					// early exit
					if(!validLeftContext) {
						break;
					}
				}
				mCurLeftSkipSize = 0;
			}
			mCurPatternSize = 1;
		}

		return false;
	}

	protected Text [] getTerms(Indexable doc) {
		String text = doc.getContent();
		return mTokenizer.processContent(text);
	}

	protected boolean getLeftContext(Text leftContext, Text [] terms, int start, int len) {
		return getSpan(leftContext, terms, start, len);
	}

	protected boolean getRightContext(Text rightContext, Text [] terms, int start, int len) {
		return getSpan(rightContext, terms, start, len);
	}

	protected boolean getPattern(Text pattern, Text [] terms, int start, int len) {
		return getSpan(pattern, terms, start, len);
	}

	private boolean getSpan(Text span, Text [] terms, int start, int len) {
		if(start < 0 || start + len > terms.length) {
			return false;
		}

		span.clear();
		for(int i = start; i < start + len; i++) {
			span.append(terms[i].getBytes(), 0, terms[i].getLength());

			if(i != start + len - 1) {
				span.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
			}
		}

		return true;
	}

	// is the current configuration the same as the current one?
	private final boolean sameAsLast() {
		return (mPrevPos == mCurPos) && (mPrevPatternSize == mCurPatternSize) && (mPrevLeftSkipSize == mCurLeftSkipSize) && (mPrevLeftContextSize == mCurLeftContextSize) && (mPrevRightSkipSize == mCurRightSkipSize) && (mPrevRightContextSize == mCurRightContextSize);
	}

	// move current configuration to previous configuration
	private final void saveConfig() {
		mPrevPos = mCurPos;
		mPrevPatternSize = mCurPatternSize;
		mPrevLeftSkipSize = mCurLeftSkipSize;
		mPrevLeftContextSize = mCurLeftContextSize;
		mPrevRightSkipSize = mCurRightSkipSize;
		mPrevRightContextSize = mCurRightContextSize;
	}
}
