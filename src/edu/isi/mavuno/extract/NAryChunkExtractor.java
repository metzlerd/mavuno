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
import edu.isi.mavuno.nlp.NLProcTools;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.PermutationGenerator;
import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.TratzParsedTokenWritable;

public class NAryChunkExtractor extends Extractor {

	private static final Text ADJACENT_PATTERN_NAME = new Text("|_|");

	private int mArity;
	private int mMaxSkipSize;

	private int [] mContextPositions;
	private int [] mPatternPositions;

	private Chunk [] mContextChunks;
	private Chunk [] mPatternChunks;

	private PermutationGenerator mPermGen;

	private Iterator<ContextPatternWritable> mChunkPairsIter = null;
	private Iterator<SentenceWritable<TratzParsedTokenWritable>> mSentIter = null;

	private final List<Chunk> mChunks = new ArrayList<Chunk>();
	private final List<TratzParsedTokenWritable> mChunkTokens = new ArrayList<TratzParsedTokenWritable>();

	private final List<ContextPatternWritable> mChunkPairs = new ArrayList<ContextPatternWritable>();

	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] args = argSpec.split(":");

		// arity of the contexts
		mArity = Integer.parseInt(args[0]);

		// context positions
		mContextPositions = new int[mArity];

		// context chunks
		mContextChunks = new Chunk[mArity];

		// pattern positions
		mPatternPositions = new int[mArity-1];

		// pattern chunks
		mPatternChunks = new Chunk[mArity-1];

		// used to generate permutations over the contexts
		mPermGen = new PermutationGenerator(mArity);

		// maximum skip size
		mMaxSkipSize = Integer.parseInt(args[1]);
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

		// get chunk ids
		List<TratzParsedTokenWritable> sentenceTokens = sentence.getTokens();
		int [] chunkIds = NLProcTools.getChunkIds(sentenceTokens);

		mChunks.clear();
		mChunkTokens.clear();

		// extract chunks from sentence
		for(int i = 0; i < chunkIds.length; i++) {
			if(i > 0 && chunkIds[i] != chunkIds[i-1]) {
				Chunk chunk = createChunk(mChunkTokens);
				mChunks.add(chunk);
				mChunkTokens.clear();
			}
			mChunkTokens.add(sentenceTokens.get(i));
		}

		// handle last chunk in sentence
		if(mChunkTokens.size() > 0) {
			Chunk chunk = createChunk(mChunkTokens);
			mChunks.add(chunk);
		}

		// there's nothing we can do if there aren't at least mArity chunks in the sentence
		if(mArity > mChunks.size()) {
			mChunkPairsIter = mChunkPairs.iterator();
			return;
		}

		// initialize context positions
		for(int i = 0; i < mArity; i++) {
			mContextPositions[i] = i;
		}

		// initialize pattern positions
		for(int i = 0; i < mArity - 1; i++) {
			mPatternPositions[i] = i;
		}

		// generate (context, pattern) pairs based on chunks
		final Text basePattern = new Text();
		while(true) {
			// construct context
			for(int i = 0; i < mArity; i++) {
				mContextChunks[i] = mChunks.get(mContextPositions[i]);
			}

			// construct pattern
			for(int i = 0; i < mArity - 1; i++) {
				mPatternChunks[i] = mChunks.get(mPatternPositions[i]);
			}

			// add (context, pattern) pair
			basePattern.clear();
			for(int i = 0; i < mArity - 1; i++) {
				// left chunk type
				basePattern.append(mContextChunks[i].type.getBytes(), 0, mContextChunks[i].type.getLength());					
				basePattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);

				if(mContextPositions[i+1] - mPatternPositions[i] > 1 || mPatternPositions[i] - mContextPositions[i] > 1) {
					if(mPatternPositions[i] == mContextPositions[i]) {
						basePattern.append(MavunoUtils.ASTERISK_BYTES, 0, MavunoUtils.ASTERISK_BYTES_LENGTH);
					}
					else if(mPatternPositions[i] == mContextPositions[i] + 1) {
						basePattern.append(mPatternChunks[i].text.getBytes(), 0, mPatternChunks[i].text.getLength());
						basePattern.append(MavunoUtils.ASTERISK_BYTES, 0, MavunoUtils.ASTERISK_BYTES_LENGTH);						
					}
					else if(mPatternPositions[i] + 1 == mContextPositions[i+1]) {
						basePattern.append(MavunoUtils.ASTERISK_BYTES, 0, MavunoUtils.ASTERISK_BYTES_LENGTH);						
						basePattern.append(mPatternChunks[i].text.getBytes(), 0, mPatternChunks[i].text.getLength());
					}
					else {
						basePattern.append(MavunoUtils.ASTERISK_BYTES, 0, MavunoUtils.ASTERISK_BYTES_LENGTH);						
						basePattern.append(mPatternChunks[i].text.getBytes(), 0, mPatternChunks[i].text.getLength());
						basePattern.append(MavunoUtils.ASTERISK_BYTES, 0, MavunoUtils.ASTERISK_BYTES_LENGTH);
					}
				}
				else if(mPatternPositions[i] == mContextPositions[i]) {
					basePattern.append(ADJACENT_PATTERN_NAME.getBytes(), 0, ADJACENT_PATTERN_NAME.getLength());
				}
				else {
					basePattern.append(mPatternChunks[i].text.getBytes(), 0, mPatternChunks[i].text.getLength());
				}
				basePattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
			}

			// last chunk type
			basePattern.append(mContextChunks[mArity-1].type.getBytes(), 0, mContextChunks[mArity-1].type.getLength());					
			basePattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);

			int [] indices;
			mPermGen.reset();
			while(mPermGen.hasMore()) {
				// get next permutation
				indices = mPermGen.getNext();

				ContextPatternWritable c = new ContextPatternWritable();

				// pattern
				c.setPattern(basePattern);
				Text numLeftText = new Text(mPermGen.getNumLeft() + "/" + mArity);
				c.getPattern().append(numLeftText.getBytes(), 0, numLeftText.getLength());

				// context
				c.getContext().clear();
				for(int i = 0; i < mArity; i++) {
					c.getContext().append(mContextChunks[indices[i]].text.getBytes(), 0, mContextChunks[indices[i]].text.getLength());
					if(i != mArity - 1) {
						c.getContext().append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
					}
				}

				// add to chunk pairs
				mChunkPairs.add(c);
			}

			// get next set of context and pattern positions
			int pos = mArity - 2;
			while(pos >= 0) {
				if(mPatternPositions[pos] + 1 < mChunks.size() && mPatternPositions[pos] + 1 < mContextPositions[pos+1]) {
					mPatternPositions[pos]++;
					for(int i = pos + 1; i < mArity - 2; i++) {
						mPatternPositions[i] = mContextPositions[i];
					}
					break;
				}
				pos--;
			}

			// update the context positions if the pattern positions can't be updated any further
			if(pos < 0) {
				pos = mArity - 1;
				while(pos >= 0) {
					if(mContextPositions[pos] + 1 < mChunks.size() && 
					   (pos + 1 >= mArity || mContextPositions[pos+1] - (mContextPositions[pos] + 1) >= 1) && 
					   (pos <= 0 || mContextPositions[pos] - mContextPositions[pos-1] - 1 <= mMaxSkipSize)) {
						mContextPositions[pos]++;
						if(pos < mArity - 1) {
							mPatternPositions[pos] = mContextPositions[pos];
						}

						for(int i = pos + 1; i < mArity; i++) {
							mContextPositions[i] = mContextPositions[pos] + (i - pos);
							if(i < mArity - 1) {
								mPatternPositions[i] = mContextPositions[i];
							}
						}

						break;
					}
					pos--;
				}


				// if neither the context nor the pattern positions can be updated then we're done
				if(pos < 0) {
					// get iterator
					mChunkPairsIter = mChunkPairs.iterator();
					return;
				}
			}
		}
	}

	private Chunk createChunk(List<TratzParsedTokenWritable> terms) {
		Chunk chunk = new Chunk();

		// construct the chunk text and detect the chunk type
		Text chunkNEType = null;
		for(int i = 0; i < terms.size(); i++) {
			Text term = terms.get(i).getToken();
			Text neTag = terms.get(i).getNETag();

			chunk.text.append(term.getBytes(), 0, term.getLength());

			if(i != terms.size() - 1) {
				chunk.text.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
			}

			// TODO: replace this with a more robust way of checking if this is an actual named entity or not
			if(neTag.getLength() != 1 || (neTag.getLength() > 0 && neTag.getBytes()[0] != 'O')) {
				chunkNEType = neTag;
			}
		}

		// set the chunk type (note that this is somewhat heuristic)
		if(chunkNEType != null) { // chunk type = named entity type, if present
			chunk.type.set(chunkNEType);
		}
		else { // otherwise, chunk type = chunk tag
			Text chunkTag = terms.get(0).getChunkTag();
			if(chunkTag.getLength() > 2) {
				chunk.type.set(chunkTag.getBytes(), 2, chunkTag.getLength()-2);
			}
			else {
				chunk.type.set(chunkTag);
			}
		}

		return chunk;
	}

	public static class Chunk {
		public final Text text = new Text();
		public final Text type = new Text();
	}
}
