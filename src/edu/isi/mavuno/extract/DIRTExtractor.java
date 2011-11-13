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
import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.TratzParsedTokenWritable;
import edu.umd.cloud9.util.array.ArrayListOfInts;

public class DIRTExtractor extends Extractor {

	// dependency parser type for punctuation
	private static final Text PUNCTUATION_TYPE = new Text("punct");

	// extractor parameters
	private boolean mOrContextStyle = false;
	private boolean mLeftOnlyContextStyle = false;
	private boolean mRightOnlyContextStyle = false;

	private boolean mHeadOnly = false;
	private boolean mUseLemmas = false;
	
	private Iterator<ContextPatternWritable> mDependPairsIter = null;
	private Iterator<SentenceWritable<TratzParsedTokenWritable>> mSentIter = null;

	private final List<ContextPatternWritable> mDependPairs = new ArrayList<ContextPatternWritable>();

	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] args = argSpec.split(":");

		// context style (left only, right only, left AND right, left OR right)
		String contextStyle = args[0].toLowerCase();

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
			throw new RuntimeException("Unrecognized DIRTExtractor context style = " + contextStyle);
		}

		// should we only retain the head of the tree?
		if(args.length == 3) {
			mHeadOnly = Boolean.parseBoolean(args[1]);
			mUseLemmas = Boolean.getBoolean(args[2]);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setDocument(Indexable doc) {
		if(doc instanceof SentenceSegmentedDocument) {
			// get sentence iterator
			mSentIter = ((SentenceSegmentedDocument<TratzParsedTokenWritable>)doc).getSentences().iterator();
		}
		else {
			throw new RuntimeException("DIRTExtractor is only applicable to SengenceSegmentedDocument<T extends TratzParsedTokenWritable> inputs!");
		}
	}

	@Override
	public boolean getNextPair(ContextPatternWritable pair) {
		if(mDependPairsIter != null && mDependPairsIter.hasNext()) {
			ContextPatternWritable c = mDependPairsIter.next();
			pair.setContext(c.getContext());
			pair.setPattern(c.getPattern());
			return true;
		}

		if(mSentIter != null && mSentIter.hasNext()) {
			loadDependPairs();
			return getNextPair(pair);
		}

		return false;
	}

	private void loadDependPairs() {
		// clear dependency pairs
		mDependPairs.clear();

		// get sentence
		SentenceWritable<TratzParsedTokenWritable> sentence = mSentIter.next();

		// get sentence tokens
		List<TratzParsedTokenWritable> tokens = sentence.getTokens();

		// get chunk ids
		int [] chunkIds = NLProcTools.getChunkIds(tokens);
		
		// get mapping from positions to chunks
		Text [] chunks = new Text[tokens.size()];

		Text curChunk = null;
		for(int i = 0; i < tokens.size(); i++) {
			Text text = tokens.get(i).getToken();
			
			if(i == 0 || (i > 0 && chunkIds[i] != chunkIds[i-1])) {
				curChunk = new Text(text);
			}
			else {
				curChunk.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
				curChunk.append(text.getBytes(), 0, text.getLength());
			}

			chunks[i] = curChunk;
		}

		// populate parse tree
		ArrayListOfInts [] children = new ArrayListOfInts[tokens.size()+1];
		for(int i = 0; i < tokens.size()+1; i++) {
			children[i] = new ArrayListOfInts();
		}

		for(int i = 0; i < tokens.size(); i++) {
			TratzParsedTokenWritable t = tokens.get(i);

			// ignore punctuation
			if(!t.getDependType().equals(PUNCTUATION_TYPE)) {
				children[t.getDependIndex()].add(i+1);
			}
		}

		// extract (context, pattern) pairs from parse tree
		for(int i = 0; i < children[0].size(); i++) {
			extractPairs(children, children[0].get(i), tokens, chunks);
		}

		// get iterator
		mDependPairsIter = mDependPairs.iterator();
	}

	private List<ArrayListOfInts> extractPairs(ArrayListOfInts [] children, int root, List<TratzParsedTokenWritable> tokens, Text[] chunks) {
		List<ArrayListOfInts> partialPaths = new ArrayList<ArrayListOfInts>();

		int [] childrenIds = children[root].getArray();

		// process non-leaf node
		for(int i = 0; i < children[root].size(); i++ ) {
			List<ArrayListOfInts> paths = extractPairs(children, childrenIds[i], tokens, chunks);
			for(ArrayListOfInts path : paths) {
				path.add(root);
			}

			// connect these paths with others that go through the current node
			for(ArrayListOfInts previousPath : partialPaths) {
				if(previousPath.size() <= 1) { // path must consist of more than just a root
					continue;
				}

				for(ArrayListOfInts path : paths) {
					if(path.size() <= 1) { // path must consist of more than just a root
						continue;
					}

					mDependPairs.addAll(getContext(previousPath, path, tokens, chunks));
					mDependPairs.addAll(getContext(path, previousPath, tokens, chunks));
				}
			}

			// add the current set of paths to the partial paths
			partialPaths.addAll(paths);
		}

		// start new path from current node
		ArrayListOfInts leaf = new ArrayListOfInts();
		leaf.add(root);
		partialPaths.add(leaf);

		return partialPaths;
	}

	private List<ContextPatternWritable> getContext(ArrayListOfInts leftPath, ArrayListOfInts rightPath, List<TratzParsedTokenWritable> tokens, Text[] chunks) { //, int leftContextSize, int rightContextSize) {
		// construct (context, pattern) pairs
		List<ContextPatternWritable> contexts = new ArrayList<ContextPatternWritable>();

		// make sure that the dimensions are feasible
		if(leftPath.size() < 1 || rightPath.size() < 1) {
			return contexts;
		}
		
		// make sure we don't split the left context's chunk
		Text leftChunk = chunks[leftPath.get(0)-1];
		for(int i = 1; i <= leftPath.size() - 1; i++) {
			if(chunks[leftPath.get(i)-1].equals(leftChunk)) {
				return contexts;
			}
		}

		// make sure we don't split the right context's chunk
		Text rightChunk = chunks[rightPath.get(0)-1];
		for(int i = rightPath.size() - 1; i >= 1; i--) {
			if(chunks[rightPath.get(i)-1].equals(rightChunk)) {
				return contexts;
			}
		}
		
		TratzParsedTokenWritable t;
		Text term, posTag, dependType;

		// construct pattern based on the parse tree path
		final Text pattern = new Text();

		// encode left context chunk type
		// TODO: replace this with a more robust way of checking if this is an actual named entity or not
		Text leftNETag = tokens.get(leftPath.get(0)-1).getNETag();
		Text leftChunkTag = tokens.get(leftPath.get(0)-1).getChunkTag();
		if(leftNETag.getLength() != 1 || (leftNETag.getLength() > 0 && leftNETag.getBytes()[0] != 'O')) {
			pattern.append(leftNETag.getBytes(), 0, leftNETag.getLength());
			pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
		}
		else {
			if(leftChunkTag.getLength() > 2) {
				pattern.append(leftChunkTag.getBytes(), 2, leftChunkTag.getLength()-2);
			}
			else {
				pattern.append(leftChunkTag.getBytes(), 0, leftChunkTag.getLength());				
			}
			pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);			
		}
		
		// left path portion of pattern
		if(!mHeadOnly) {
			for(int i = 0; i <= leftPath.size() - 2; i++) {
				t = tokens.get(leftPath.get(i)-1);

				term = mUseLemmas ? t.getLemma() : t.getToken();
				dependType = t.getDependType();
				posTag = t.getPosTag();

				if(i != 0) {
					pattern.append(term.getBytes(), 0, term.getLength());
					pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
					pattern.append(posTag.getBytes(), 0, posTag.getLength() > 2 ? 2 : posTag.getLength());
					pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
				}

				pattern.append(dependType.getBytes(), 0, dependType.getLength());
				pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);

			}
		}
		else {
			dependType = tokens.get(leftPath.get(0)-1).getDependType();
			posTag = tokens.get(leftPath.get(0)-1).getPosTag();

			pattern.append(dependType.getBytes(), 0, dependType.getLength());
			pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);			
		}

		// root portion of pattern
		if(leftPath.get(leftPath.size()-1) != rightPath.get(rightPath.size()-1)) {
			throw new RuntimeException("Left and right paths do not share the same root! -- " + leftPath + " <--> " + rightPath);
		}

		t = tokens.get(leftPath.get(leftPath.size()-1)-1);

		term = mUseLemmas ? t.getLemma() : t.getToken();
		dependType = t.getDependType();
		posTag = t.getPosTag();

		pattern.append(posTag.getBytes(), 0, posTag.getLength() > 2 ? 2 : posTag.getLength());
		pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
		pattern.append(term.getBytes(), 0, term.getLength());
		pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
		pattern.append(posTag.getBytes(), 0, posTag.getLength() > 2 ? 2 : posTag.getLength());

		// right path portion of pattern
		if(!mHeadOnly) {
			for(int i = rightPath.size() - 2; i >= 0; i--) {
				t = tokens.get(rightPath.get(i)-1);

				term = mUseLemmas ? t.getLemma() : t.getToken();
				dependType = t.getDependType();
				posTag = t.getPosTag();

				pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
				pattern.append(dependType.getBytes(), 0, dependType.getLength());

				if(i != 0) {
					pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
					pattern.append(posTag.getBytes(), 0, posTag.getLength() > 2 ? 2 : posTag.getLength());
					pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
					pattern.append(term.getBytes(), 0, term.getLength());
				}
			}
		}
		else {
			dependType = tokens.get(rightPath.get(0)-1).getDependType();
			posTag = tokens.get(rightPath.get(0)-1).getPosTag();

			pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
			pattern.append(dependType.getBytes(), 0, dependType.getLength());
		}

		// encode right context chunk type
		// TODO: replace this with a more robust way of checking if this is an actual named entity or not
		Text rightNETag = tokens.get(rightPath.get(0)-1).getNETag();
		Text rightChunkTag = tokens.get(rightPath.get(0)-1).getChunkTag();
		if(rightNETag.getLength() != 1 || (rightNETag.getLength() > 0 && rightNETag.getBytes()[0] != 'O')) {
			pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);
			pattern.append(rightNETag.getBytes(), 0, rightNETag.getLength());
		}
		else {
			pattern.append(MavunoUtils.PIPE_BYTES, 0, MavunoUtils.PIPE_BYTES_LENGTH);			
			if(rightChunkTag.getLength() > 2) {
				pattern.append(rightChunkTag.getBytes(), 2, rightChunkTag.getLength()-2);
			}
			else {
				pattern.append(rightChunkTag.getBytes(), 0, rightChunkTag.getLength());				
			}
		}
		
		if(mOrContextStyle) {
			if(!mRightOnlyContextStyle) {
				ContextPatternWritable c = new ContextPatternWritable();
				c.setContext(MavunoUtils.createContext(leftChunk, MavunoUtils.ASTERISK));
				c.setPattern(pattern);
				contexts.add(c);				
			}
			if(!mLeftOnlyContextStyle) {
				ContextPatternWritable c = new ContextPatternWritable();
				c.setContext(MavunoUtils.createContext(MavunoUtils.ASTERISK, rightChunk));
				c.setPattern(pattern);
				contexts.add(c);				
			}
		}
		else {
			ContextPatternWritable c = new ContextPatternWritable();
			c.setContext(MavunoUtils.createContext(leftChunk, rightChunk));
			c.setPattern(pattern);
			contexts.add(c);
		}
		
		return contexts;
	}

}
