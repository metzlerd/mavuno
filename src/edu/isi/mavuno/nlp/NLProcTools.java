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

package edu.isi.mavuno.nlp;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import opennlp.tools.chunker.Chunker;
import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;

import org.apache.hadoop.io.Text;

import tratz.jwni.WordNet;
import tratz.ml.LinearClassificationModel;
import tratz.parse.NLParser;
import tratz.parse.featgen.ParseFeatureGenerator;
import tratz.parse.ml.ParseModel;
import tratz.parse.types.Arc;
import tratz.parse.types.Parse;
import tratz.parse.types.Sentence;
import tratz.parse.types.Token;
import tratz.parse.util.NLParserUtils;
import tratz.pos.PosTagger;
import tratz.pos.featgen.PosFeatureGenerator;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.TratzParsedTokenWritable;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.StripTagsProcessor;
import edu.stanford.nlp.process.WordToSentenceProcessor;

/**
 * @author metzler
 *
 */
public class NLProcTools {

	// maximum sentence length to process
	public static final int MAX_SENTENCE_LENGTH = 100;

	public static final String DEFAULT_CHUNKER_MODEL = "models/en-chunker.bin";
	public static final String DEFAULT_NER_MODEL = "models/muc.7class.distsim.crf.ser.gz";
	public static final String DEFAULT_SENTENCE_MODEL = "models/en-sent.bin";
	public static final String DEFAULT_TRATZ_POS_MODEL = "models/tratzPosTaggingModel.gz";
	public static final String DEFAULT_TRATZ_PARSER_MODEL = "models/tratzParseModel.gz";

	// directory to unpack wordnet data files into
	public static final String WORDNET_PATH = "wordnet";

	// regular expressions (defined over POS tag streams) for composite and basic terms
	private static final Pattern BASIC_TERM_PATTERN = Pattern.compile("D?((A|N)+|((A|N)*(NP)?)(A|N)*)N");
	private static final Pattern COMPOSITE_TERM_PATTERN = Pattern.compile("(X|D)?((A|N)+|((A|N)*(NP)?)(A|N)*)NX*(C+(X|D)?((A|N)+|((A|N)*(NP)?)(A|N)*)NX*)*");

	public static final String SEPARATOR = "\t";

	// part of speech tagger
	private PosTagger mPosTagger = null;

	// chunker
	private Chunker mChunker = null;

	// named entity tagger
	private AbstractSequenceClassifier<CoreLabel> mNETagger = null;

	// parser
	private NLParser mParser = null;

	// sentence detector
	final private WordToSentenceProcessor<Word> mSentenceDetector = new WordToSentenceProcessor<Word>();

	// sgml tag stripper
	@SuppressWarnings("rawtypes")
	final private StripTagsProcessor mTagStripper = new StripTagsProcessor(true);

	// sentence words to process
	private final List<Word> mSentenceWords = new ArrayList<Word>();

	// sentence tokens to process
	private final List<Token> mSentenceTokens = new ArrayList<Token>();

	// sentence to process (based on the current set of tokens)
	private final Sentence mSentence = new Sentence(mSentenceTokens);

	// chunker words
	private final List<String> mChunkerWords = new ArrayList<String>();

	// pos tags
	private final List<String> mPosTags = new ArrayList<String>();

	// pointer from token to head arc
	private final Map<Token, Arc> mTokenToHeadArc = new HashMap<Token, Arc>();

	public void setSentence(List<Word> sentence) {
		// set the sentence words
		mSentenceWords.clear();
		mSentenceWords.addAll(sentence);

		// clear sentence tokens
		mSentenceTokens.clear();

		// convert Words to Tokens
		int pos = 1;
		for(Word word : sentence) {
			Token tok = new Token(word.word(), pos);
			mSentenceTokens.add(tok);
			pos++;
		}
	}

	// initialize part-of-speech tagger
	public void initializePOSTagger() throws IOException, ClassNotFoundException {
		InputStream is = new GZIPInputStream(getClass().getClassLoader().getResourceAsStream(DEFAULT_TRATZ_POS_MODEL));
		ObjectInputStream ois = new ObjectInputStream(is);
		LinearClassificationModel model = (LinearClassificationModel)ois.readObject();
		PosFeatureGenerator featGenerator = (PosFeatureGenerator)ois.readObject();
		ois.close();

		mPosTagger = new PosTagger(model, featGenerator);
	}

	// initialize WordNet
	public void initializeWordNet() throws IOException {
		URL indexURL = getClass().getClassLoader().getResource("wordnet/index.sense");
		URL nounDataURL = getClass().getClassLoader().getResource("wordnet/data.noun");
		URL verbDataURL = getClass().getClassLoader().getResource("wordnet/data.verb");
		URL adjDataURL = getClass().getClassLoader().getResource("wordnet/data.adj");
		URL advDataURL = getClass().getClassLoader().getResource("wordnet/data.adv");
		URL nounIndexURL = getClass().getClassLoader().getResource("wordnet/index.noun");
		URL verbIndexURL = getClass().getClassLoader().getResource("wordnet/index.verb");
		URL adjIndexURL = getClass().getClassLoader().getResource("wordnet/index.adj");
		URL advIndexURL = getClass().getClassLoader().getResource("wordnet/index.adv");
		URL nounExceptionsURL = getClass().getClassLoader().getResource("wordnet/noun.exc");
		URL verbExceptionsURL = getClass().getClassLoader().getResource("wordnet/verb.exc");
		URL adjExceptionsURL = getClass().getClassLoader().getResource("wordnet/adj.exc");
		URL lexnamesURL = getClass().getClassLoader().getResource("wordnet/lexnames");
		
		new WordNet(indexURL, nounDataURL, verbDataURL, adjDataURL, advDataURL,
					nounIndexURL, verbIndexURL, adjIndexURL, advIndexURL, 
					nounExceptionsURL, verbExceptionsURL, adjExceptionsURL,
					lexnamesURL, true);
	}

	// initialize chunker
	public void initializeChunker() throws IOException {
		mChunker = new ChunkerME(new ChunkerModel(getClass().getClassLoader().getResourceAsStream(NLProcTools.DEFAULT_CHUNKER_MODEL)));
	}

	// initialize named entity tagger
	@SuppressWarnings("unchecked")
	public void initializeNETagger() throws IOException, ClassCastException, ClassNotFoundException {
		mNETagger = CRFClassifier.getClassifier(new GZIPInputStream(getClass().getClassLoader().getResourceAsStream(NLProcTools.DEFAULT_NER_MODEL)));
	}

	// initialize parser
	public void initializeTratzParser() throws IOException, ClassNotFoundException {
		InputStream is = new GZIPInputStream(getClass().getClassLoader().getResourceAsStream(DEFAULT_TRATZ_PARSER_MODEL));
		ObjectInputStream ois = new ObjectInputStream(is);
		ParseModel model = (ParseModel)ois.readObject();
		ParseFeatureGenerator featGenerator = (ParseFeatureGenerator)ois.readObject();
		ois.close();

		mParser = new NLParser(model, featGenerator);
	}

	public List<String> getPosTags() {
		mPosTags.clear();

		// part of speech tag sentence
		mPosTagger.posTag(mSentenceTokens);

		// extract part of speech tags
		for(Token t : mSentenceTokens) {
			mPosTags.add(t.getPos());
		}

		return mPosTags;
	}

	public List<String> getChunkTags() {
		// the chunker requires the output of the pos tagger
		if(mPosTags.size() == 0) {
			getPosTags();
		}

		mChunkerWords.clear();

		for(Word w : mSentenceWords) {
			mChunkerWords.add(w.word());
		}

		return Arrays.asList(mChunker.chunk(mChunkerWords.toArray(new String[mChunkerWords.size()]), mPosTags.toArray(new String[mPosTags.size()])));
	}

	public List<String> getNETags() {
		List<String> neTags = new ArrayList<String>();

		List<CoreLabel> labels = mNETagger.classifySentence(mSentenceWords);
		for(CoreLabel label : labels) {
			neTags.add(label.get(AnswerAnnotation.class));
		}

		return neTags;
	}

	@SuppressWarnings("unchecked")
	public Map<Token, Arc> getTratzParseTree() throws Exception {
		Parse parse = mParser.parseSentence(mSentence);

		List<Arc> [] arcs = parse.getDependentArcLists();

		mTokenToHeadArc.clear();
		for(int i = 0; i < mSentenceTokens.size(); i++) {
			List<Arc> arcList = arcs[i+1];
			if(arcList != null) {
				for(Arc a : arcList) {
					mTokenToHeadArc.put(a.getChild(), a);
				}
			}
		}

		return mTokenToHeadArc;
	}

	public List<String> getLemmas(List<Word> sentence) {
		List<String> lemmas = new ArrayList<String>();

		// get the lemmas
		for(Token t : mSentenceTokens) {
			lemmas.add(NLParserUtils.getLemma(t, WordNet.getInstance()));
		}

		return lemmas;
	}

	public List<Token> getSentenceTokens() {
		return mSentenceTokens;
	}

	@SuppressWarnings("unchecked")
	public List<List<Word>> getTagStrippedSentences(String text) {
		// tokenize the document
		PTBTokenizer<Word> tokenizer = PTBTokenizer.newPTBTokenizer(new StringReader(text));
		List<Word> documentWords = tokenizer.tokenize();

		// strip tags and detect sentences
		return mSentenceDetector.process(mTagStripper.process(documentWords));
	}

	public static List<TratzParsedTokenWritable> getTaggedTokens(List<Token> tokens, List<String> posTags, List<String> neTags) {
		// make sure that # tokens = # pos tags = # ne tags
		if(tokens.size() != posTags.size() || posTags.size() != neTags.size()) {
			throw new IllegalArgumentException("Arguments must satisfy the condition: tokens.size() == posTags.size() == neTags.size()");
		}
		
		List<TratzParsedTokenWritable> taggedTokens = new ArrayList<TratzParsedTokenWritable>();
		
		for(int i = 0; i < tokens.size(); i++) {
			// construct tagged token
			TratzParsedTokenWritable t = new TratzParsedTokenWritable();
			t.setToken(tokens.get(i).getText());
			t.setPosTag(posTags.get(i));
			t.setNETag(neTags.get(i));
			
			taggedTokens.add(t);
		}
		
		return taggedTokens;
	}

	public static int [] getChunkIds(List<TratzParsedTokenWritable> tokens) {
		return getChunkIds(tokens, false);
	}
	
	public static int [] getChunkIds(List<TratzParsedTokenWritable> tokens, boolean composite) {
		int [] chunks = new int[tokens.size()];

		// get part of speech tag "signature" for the entire sentence
		String posSignature = getPOSSignature(tokens, 0, tokens.size());

		// get the appropriate term matcher
		Matcher m;
		if(composite) {
			m = COMPOSITE_TERM_PATTERN.matcher(posSignature);
		}
		else {
			m = BASIC_TERM_PATTERN.matcher(posSignature);
		}
		
		int lastMatch = 0;
		int curChunk = 0;
		while(m.find()) {
			// if there's a gap between this match and the last one
			if(m.start() > lastMatch) {
				curChunk++;
				for(int i = lastMatch; i < m.start(); i++) {
					chunks[i] = curChunk;
				}
			}

			// create new chunk for the match
			curChunk++;
			for(int i = m.start(); i < m.end(); i++) {
				chunks[i] = curChunk;
			}

			lastMatch = m.end();
		}

		// create a new chunk from the remaining text
		if(lastMatch < tokens.size()) {
			curChunk++;
			for(int i = lastMatch; i < tokens.size(); i++) {
				chunks[i] = curChunk;
			}
		}

		return chunks;
	}

	public static Set<Text> extractChunks(int id, int [] chunkIds, List<TratzParsedTokenWritable> tokens, boolean split, boolean extractSpecific, boolean extractGeneral, boolean appendPOSTag) {
		Set<Text> chunks = new HashSet<Text>();

		// get start and end position of the chunk with this id
		int chunkStart = chunkIds.length;
		int chunkEnd = -1;

		for(int i = 0; i < chunkIds.length; i++) {
			if(chunkIds[i] == id) {
				if(i < chunkStart) {
					chunkStart = i;
				}
				if(i > chunkEnd) {
					chunkEnd = i;
				}
			}
		}

		// extract "sub" chunks (using BASIC_TERM_PATTERN) from main chunk
		Set<Text> subChunks = extractSubChunks(chunkStart, chunkEnd, tokens, split, extractSpecific, extractGeneral, appendPOSTag);
		chunks.addAll(subChunks);

		return chunks;
	}

	private static Set<Text> extractSubChunks(int chunkStart, int chunkEnd, List<TratzParsedTokenWritable> tokens, boolean split, boolean extractSpecific, boolean extractGeneral, boolean appendPOSTag) {

		// make sure that there's something to extract
		if(!extractSpecific && !extractGeneral) {
			throw new IllegalArgumentException("Either extractSpecific or extractGeneral must be set to true!");
		}

		Set<Text> chunks = new HashSet<Text>();

		// get part of speech tag "signature" for this chunk
		String posSignature = getPOSSignature(tokens, chunkStart, chunkEnd+1);

		Matcher m = BASIC_TERM_PATTERN.matcher(posSignature);

		// if the basic pattern matches the entire POS signature, then this is not a composite chunk (i.e., it cannot be split)
		if(m.matches()) {
			// extract "main" chunk (if necessary)
			if(extractSpecific) {
				Text mainChunk = extractMainChunk(chunkStart, chunkEnd+1, tokens, appendPOSTag);
				if(mainChunk != null) {
					chunks.add(mainChunk);
				}
			}

			// extract "generalized" chunk from main chunk (if necessary)
			if(extractSpecific) {
				Text generalizedChunk = extractGeneralizedChunk(chunkStart, chunkEnd, tokens, appendPOSTag);
				if(generalizedChunk != null) {
					chunks.add(generalizedChunk);
				}
			}

			return chunks;
		}

		List<Text> mainSubChunks = new ArrayList<Text>();
		List<Text> generalizedSubChunks = new ArrayList<Text>();

		m.reset();
		while(m.find()) {
			int start = m.start() + chunkStart;
			int end = m.end() + chunkStart;

			// extract "main" sub chunk (if necessary)
			if(extractSpecific) {
				Text mainChunk = extractMainChunk(start, end, tokens, appendPOSTag);
				if(mainChunk != null) {
					if(split) {
						chunks.add(mainChunk);
					}
					else {
						mainSubChunks.add(mainChunk);
					}
				}
			}

			// extract "generalized" chunk for this sub chunk (if necessary)
			if(extractGeneral) {
				Text generalizedChunk = extractGeneralizedChunk(start, end-1, tokens, appendPOSTag);
				if(generalizedChunk != null) {
					if(split) {
						chunks.add(generalizedChunk);
					}
					else {
						generalizedSubChunks.add(generalizedChunk);
					}
				}
			}
		}

		// add composite (i.e., non-split) chunks
		if(!split) {
			// create composite chunk from main sub-chunks
			if(mainSubChunks.size() > 0) {
				chunks.add(MavunoUtils.createContext(SEPARATOR, mainSubChunks.toArray(new Text[mainSubChunks.size()])));
			}

			if(generalizedSubChunks.size() > 0) {
				// create composite chunk from generalized sub-chunks
				chunks.add(MavunoUtils.createContext(SEPARATOR, generalizedSubChunks.toArray(new Text[generalizedSubChunks.size()])));
			}
		}

		return chunks;
	}

	private static Text extractMainChunk(int start, int end, List<TratzParsedTokenWritable> tokens, boolean appendPOSTag) {
		StringBuffer chunk = new StringBuffer();
		String tag = "O";

		boolean firstWord = true;
		for(int i = start; i < end; i++) {
			TratzParsedTokenWritable t = tokens.get(i);
			
			String text = t.getToken().toString();
			String neTag = t.getNETag().toString();

			// exclude special characters and punctuation from the beginning and end of the chunk
			if(isPunctuation(text)) {
				continue;
			}

			// get (approximate) NE type
			if(!"O".equals(neTag)) {
				tag = neTag;
			}

			if(firstWord) {
				firstWord = false;
			}
			else {
				chunk.append(' ');
			}

			chunk.append(text);
		}

		// return null if no valid terms are found
		if(chunk.length() == 0) {
			return null;
		}

		if(appendPOSTag) {
			chunk.append("|");
			chunk.append(tag);
		}

		return new Text(chunk.toString());
	}

	private static Text extractGeneralizedChunk(int chunkStart, int chunkEnd, List<TratzParsedTokenWritable> tokens, boolean appendPOSTag) {
		Text chunk = new Text();

		int startPos;
		for(startPos = chunkEnd; startPos >= chunkStart; startPos--) {
			if(!tokens.get(startPos).getPosTag().toString().startsWith("NN")) {
				break;
			}
		}
		
		if(startPos != chunkStart-1 && startPos != chunkEnd) {
			Text generalChunk = extractMainChunk(startPos+1, chunkEnd+1, tokens, appendPOSTag);
			if(generalChunk != null) {
				chunk.set(generalChunk);
			}
		}

		// return null if no valid terms are found
		if(chunk.getLength() == 0) {
			return null;
		}

		return chunk;
	}

	private static String getPOSSignature(List<TratzParsedTokenWritable> tokens, int chunkStart, int chunkEnd) {
		String signature = "";

		for(int i = chunkStart; i < chunkEnd; i++) {
			TratzParsedTokenWritable t = tokens.get(i);

			String term = t.getToken().toString();
			String posTag = t.getPosTag().toString();
			String neTag = t.getNETag().toString();

			if(neTag.equals("LOCATION") || neTag.equals("PERSON") || neTag.equals("ORGANIZATION")) {
				signature += "N";
			}
			else if(posTag.startsWith("NN") || posTag.equals("VBG") || posTag.equals("POS") || posTag.equals("CD") || posTag.startsWith("PR") || posTag.startsWith("WP")) {
				signature += "N";
			}
			else if(posTag.startsWith("JJ")) {
				signature += "A";
			}
			else if(posTag.startsWith("IN") || posTag.startsWith("TO")) {
				signature += "P";
			}
			else if(term.equals(",") || term.equals("and")) {
				signature += "C";
			}
			else if(posTag.charAt(0) < 'A' || posTag.charAt(0) > 'Z') {
				signature += "X";
			}
			else if(posTag.equals("DT")) {
				signature += "D";
			}
			else {
				signature += "O";
			}
		}

		return signature;
	}

	private static boolean isPunctuation(String text) {
		boolean isPunc = true;

		// check for special treebank punctuation tokens
		if("-LRB-".equals(text) || "-RRB-".equals(text) || 
		   "-LCB-".equals(text) || "-RCB-".equals(text) || 
		   "-LSB-".equals(text) || "-RSB-".equals(text)) {
			return true;
		}
		
		// text is considered to be punctuation if it doesn't contain any alphanumeric characters
		for(int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
				isPunc = false;
				break;
			}
		}

		return isPunc;
	}

}
