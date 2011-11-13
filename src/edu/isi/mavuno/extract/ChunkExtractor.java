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
import edu.isi.mavuno.input.SentenceSegmentedDocument;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.TratzParsedTokenWritable;

public class ChunkExtractor extends Extractor {

	private static final Text ADJACENT_PATTERN_NAME = new Text("|_|");

	private static final Text REVERSE_PATTERN = new Text("|-");
	
	private int mMaxSkipSize;

	private boolean mSurfaceForms = false;
	
	private boolean mOrContextStyle = false;
	private boolean mLeftOnlyContextStyle = false;
	private boolean mRightOnlyContextStyle = false;

	private Iterator<ContextPatternWritable> mChunkPairsIter = null;
	private Iterator<SentenceWritable<TratzParsedTokenWritable>> mSentIter = null;

	private final List<Text> mChunks = new ArrayList<Text>();
	private final List<Text> mChunkTokens = new ArrayList<Text>();
	private final Text mChunkType = new Text();

	private final List<ContextPatternWritable> mChunkPairs = new ArrayList<ContextPatternWritable>();

	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] args = argSpec.split(":");

		// use surface form?
		mSurfaceForms = Boolean.parseBoolean(args[0]);
		
		// maximum skip size
		mMaxSkipSize = Integer.parseInt(args[1]);

		// context style (left, right, left OR right, left AND right)
		String contextStyle = args[2].toLowerCase();

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
			throw new RuntimeException("Unrecognized ChunkExtractor context style = " + contextStyle);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setDocument(final Indexable doc) {
		if(doc instanceof SentenceSegmentedDocument) {
			// get sentence iterator
			mSentIter = ((SentenceSegmentedDocument<TratzParsedTokenWritable>)doc).getSentences().iterator();
		}
		else {
			throw new RuntimeException("ChunkExtractor is only applicable to SengenceSegmentedDocument<T extends TratzParsedTokenWritable> inputs!");
		}
	}

	@Override
	public boolean getNextPair(final ContextPatternWritable pair) {
		if(mChunkPairsIter != null && mChunkPairsIter.hasNext()) {
			ContextPatternWritable c = mChunkPairsIter.next();
			pair.setContext(c.getContext());
			pair.setPattern(c.getPattern());
			return true;
		}

		if(mSentIter != null && mSentIter.hasNext()) {
			loadChunkPairs();
			return getNextPair(pair);
		}

		return false;
	}

	private void loadChunkPairs() {
		// clear chunk pairs
		mChunkPairs.clear();

		// get sentence
		SentenceWritable<TratzParsedTokenWritable> sentence = mSentIter.next();

		// extract chunks from sentence
		mChunks.clear();
		mChunkTokens.clear();
		List<TratzParsedTokenWritable> tokens = sentence.getTokens();
		Text lastNETag = new Text();
		for(int i = 0; i < tokens.size(); i++) {
			TratzParsedTokenWritable t = tokens.get(i);
			byte chunkType = t.getChunkTag().getLength() > 0 ? t.getChunkTag().getBytes()[0] : 0;
			Text neTag = t.getNETag();
			if(neTag.compareTo(lastNETag.getBytes(), 0, lastNETag.getLength()) != 0 || (neTag.getLength() == 1 && (neTag.getLength() > 0 && neTag.getBytes()[0] == 'O')) && (chunkType == 'B' || chunkType == 'O')) {
				if(mChunkTokens.size() > 0) { // && mChunkType.getBytes()[0] != 'O') {
					Text chunk = createChunk(mChunkTokens, mChunkType);
					mChunks.add(chunk);
				}
				mChunkTokens.clear();
				mChunkType.set(t.getChunkTag());
			}
			mChunkTokens.add(t.getToken());
			lastNETag.set(neTag);
		}

		// handle last chunk in sentence
		if(mChunkTokens.size() > 0) { // && mChunkType.getBytes()[0] != 'O') {
			Text chunk = createChunk(mChunkTokens, mChunkType);
			mChunks.add(chunk);
		}

		// generate adjacent (context, pattern) pairs
		for(int patternPos = 0; patternPos < mChunks.size() - 1; patternPos++) {
			Text leftPattern = new Text();
			leftPattern.append(mChunks.get(patternPos).getBytes(), 0, mChunks.get(patternPos).getLength());
			leftPattern.append(ADJACENT_PATTERN_NAME.getBytes(), 0, ADJACENT_PATTERN_NAME.getLength());
			addPair(mChunks.get(patternPos), leftPattern, mChunks.get(patternPos+1));

			Text rightPattern = new Text();
			rightPattern.append(ADJACENT_PATTERN_NAME.getBytes(), 0, ADJACENT_PATTERN_NAME.getLength());
			rightPattern.append(mChunks.get(patternPos+1).getBytes(), 0, mChunks.get(patternPos+1).getLength());
			addPair(mChunks.get(patternPos), rightPattern, mChunks.get(patternPos+1));
		}
		
		// generate non-adjacent (context, pattern) pairs based on chunks
		for(int patternPos = 0; patternPos < mChunks.size(); patternPos++) {
			for(int leftSkip = 0; leftSkip <= mMaxSkipSize; leftSkip++) {
				if(patternPos-leftSkip-1 < 0) {
					continue;
				}

				if(mOrContextStyle && !mRightOnlyContextStyle) {
					addPair(mChunks.get(patternPos-leftSkip-1), mChunks.get(patternPos), ContextPatternWritable.ASTERISK);
				}

				if(mOrContextStyle && mLeftOnlyContextStyle) {
					continue;
				}

				for(int rightSkip = 0; rightSkip <= mMaxSkipSize; rightSkip++) {
					if(patternPos+rightSkip+1 >= mChunks.size()) {
						continue;
					}

					// construct (context, pattern) pair
					if(mOrContextStyle) {
						addPair(ContextPatternWritable.ASTERISK, mChunks.get(patternPos), mChunks.get(patternPos+rightSkip+1));
					}
					else {
						addPair(mChunks.get(patternPos-leftSkip-1), mChunks.get(patternPos), mChunks.get(patternPos+rightSkip+1));
					}
				}
			}
		}

		// get iterator
		mChunkPairsIter = mChunkPairs.iterator();
	}

	private void addPair(Text left, Text pattern, Text right) {
		ContextPatternWritable c;
		
		// forward pattern
		c = new ContextPatternWritable();
		c.setContext(MavunoUtils.createContext(left, right));
		c.setPattern(pattern);

		// add to chunk pairs
		mChunkPairs.add(c);

		// reverse pattern
		c = new ContextPatternWritable();
		c.setContext(MavunoUtils.createContext(right, left));
		c.setPattern(REVERSE_PATTERN);
		c.getPattern().append(pattern.getBytes(), 0, pattern.getLength());

		// add to chunk pairs
		mChunkPairs.add(c);
}
	
	private Text createChunk(List<Text> terms, Text type) {
		Text t = new Text();
		
		for(int i = 0; i < terms.size(); i++) {
			Text term = terms.get(i);
			
			t.append(term.getBytes(), 0, term.getLength());

			if(i != terms.size() - 1) {
				t.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
			}
		}
		
		if(t.getLength() > 0 && !mSurfaceForms) {
			t.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
			if(type.getLength() > 2) {
				t.append(type.getBytes(), 2, type.getLength()-2);
			}
		}

		return t;
	}
}
