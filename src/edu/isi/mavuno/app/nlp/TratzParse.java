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

package edu.isi.mavuno.app.nlp;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tratz.parse.types.Arc;
import tratz.parse.types.Token;
import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.input.TratzParsedDocument;
import edu.isi.mavuno.nlp.NLProcTools;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.TokenFactory;
import edu.isi.mavuno.util.TratzParsedTokenWritable;
import edu.stanford.nlp.ling.Word;

/**
 * @author metzler
 *
 */
public class TratzParse extends Configured implements Tool {

	// misc. counters
	private static enum StatCounters {
		TOTAL_DOCUMENTS,
		TOTAL_SENTENCES,
		TOTAL_TOKENS,
		TOTAL_DROPPED_SENTENCES,
		TOKENIZE_TIME,
		POSTAG_TIME,
		CHUNK_TIME,
		NETAG_TIME,
		PARSE_TIME
	}

	private static final Logger sLogger = Logger.getLogger(TratzParse.class);

	// token factory for TratzParsedTokenWritables
	private static final TokenFactory<TratzParsedTokenWritable> TOKEN_FACTORY = new TratzParsedTokenWritable.ParsedTokenFactory();

	public TratzParse(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, Text, TratzParsedDocument> {

		// document id
		private final Text mKey = new Text();

		// text utility class
		private final NLProcTools mTextUtils = new NLProcTools();

		// parsed document
		private final TratzParsedDocument mDoc = new TratzParsedDocument();

		@Override
		public void setup(Mapper<Writable, Indexable, Text, TratzParsedDocument>.Context context) throws IOException {
			// initialize WordNet (needed by POS tagger)
			try {
				mTextUtils.initializeWordNet();
			}
			catch(Exception e) {
				throw new RuntimeException("Error initializing WordNet instance -- " + e);
			}

			// initialize POS tagger
			try {
				mTextUtils.initializePOSTagger();
			}
			catch(Exception e) {
				throw new RuntimeException("Error initializing POS tagger -- " + e);
			}

			// initialize chunker
			try {
				mTextUtils.initializeChunker();
			}
			catch(Exception e) {
				throw new RuntimeException("Error initializing chunker -- " + e);
			}

			// initialize named entity tagger
			try {
				mTextUtils.initializeNETagger();
			}
			catch(Exception e) {
				throw new RuntimeException("Error initializing named entity tagger -- " + e);
			}

			// initialize parser
			try {
				mTextUtils.initializeTratzParser();
			}
			catch(Exception e) {
				throw new RuntimeException("Error initializing Tratz parser -- " + e);
			}
		}

		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, Text, TratzParsedDocument>.Context context) throws IOException, InterruptedException {
			// used for profiling
			long startTime;
			long endTime;

			// get the document id
			String docId = doc.getDocid();

			// get the document content
			String text = doc.getContent();

			// require both a document id and some non-null content
			if(docId == null || text == null) {
				return;
			}

			sLogger.info("Currently parsing document " + docId);

			// initialize document
			mDoc.clear();

			// set document id
			mDoc.setDocId(docId);

			// segment the document into sentences
			startTime = System.currentTimeMillis();
			List<List<Word>> sentences = mTextUtils.getTagStrippedSentences(text);
			endTime = System.currentTimeMillis();
			context.getCounter(StatCounters.TOKENIZE_TIME).increment(endTime - startTime);

			// process each sentence
			for(List<Word> sentence : sentences) {
				try {
					// skip very long sentences
					if(sentence.size() > NLProcTools.MAX_SENTENCE_LENGTH) {
						context.getCounter(StatCounters.TOTAL_DROPPED_SENTENCES).increment(1L);
						continue;
					}

					// set the sentence to be processed
					mTextUtils.setSentence(sentence);

					// part of speech tag sentence
					startTime = System.currentTimeMillis();
					List<String> posTags = mTextUtils.getPosTags();
					endTime = System.currentTimeMillis();
					context.getCounter(StatCounters.POSTAG_TIME).increment(endTime - startTime);

					// chunk sentence
					startTime = System.currentTimeMillis();
					List<String> chunkTags = mTextUtils.getChunkTags();
					endTime = System.currentTimeMillis();
					context.getCounter(StatCounters.CHUNK_TIME).increment(endTime - startTime);				

					// tag named entities
					startTime = System.currentTimeMillis();
					List<String> neTags = mTextUtils.getNETags();
					endTime = System.currentTimeMillis();
					context.getCounter(StatCounters.NETAG_TIME).increment(endTime - startTime);

					// parse sentence
					startTime = System.currentTimeMillis();
					Map<Token, Arc> tokenToHeadArc = mTextUtils.getTratzParseTree();
					endTime = System.currentTimeMillis();
					context.getCounter(StatCounters.PARSE_TIME).increment(endTime - startTime);

					// get lemmas
					List<String> lemmas = mTextUtils.getLemmas(sentence);

					// get a new parsed sentence from the token factory
					SentenceWritable<TratzParsedTokenWritable> parsedSentence = new SentenceWritable<TratzParsedTokenWritable>(TOKEN_FACTORY);

					// generate parsed tokens
					int sentencePos = 0;
					for(Token t : mTextUtils.getSentenceTokens()) {
						Arc headArc = tokenToHeadArc.get(t);

						// get a new parsed token
						TratzParsedTokenWritable token = new TratzParsedTokenWritable();

						// set the attributes of the parsed token
						token.setToken(t.getText());
						token.setCharOffset(sentence.get(sentencePos).beginPosition(), sentence.get(sentencePos).endPosition()-1);
						token.setLemma(lemmas.get(sentencePos));
						token.setPosTag(posTags.get(sentencePos)); 
						token.setChunkTag(chunkTags.get(sentencePos));
						token.setNETag(neTags.get(sentencePos));

						// dependency parse information
						if(headArc == null) {
							token.setDependType("root");
							token.setDependIndex(0);
						}
						else {
							token.setDependType(headArc.getDependency());
							token.setDependIndex(headArc.getHead().getIndex());
						}

						// if this token has disambiguation information, then append it to the POS tag
						if(headArc != null && headArc.getChild().getLexSense() != null) {
							token.setPosTag(t.getPos() + "-" + headArc.getChild().getLexSense());
						}

						// add token to sentence
						parsedSentence.addToken(token);

						// increment position within sentence
						sentencePos++;
					}

					// increment token counter
					context.getCounter(StatCounters.TOTAL_TOKENS).increment(sentence.size());

					// add sentence to document
					mDoc.addSentence(parsedSentence);
					context.getCounter(StatCounters.TOTAL_SENTENCES).increment(1L);

					// let hadoop know we're making progress to avoid a timeout
					context.progress();
				}
				catch(Exception e) {
					sLogger.info("Error parsing sentence: " + e);
				}
			}

			// set key (= doc id)
			mKey.set(docId);
			context.write(mKey, mDoc);
			context.getCounter(StatCounters.TOTAL_DOCUMENTS).increment(1L);
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.Parse", getConf());
		return run();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.Parse.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.Parse.CorpusClass", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.Parse.OutputPath", conf);

		// optional parameter that allows the parsed documents to be output in text format
		String textOutput = MavunoUtils.getOptionalParam("Mavuno.Parse.TextOutputFormat", conf);
		boolean textOutputFormat = false;
		if(textOutput != null && Boolean.parseBoolean(textOutput)) {
			textOutputFormat = true;
		}

		sLogger.info("Tool name: TratzParse");
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("TratzParse");

		MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));

		// output format -- either plain text or sequencefile (default)
		if(textOutputFormat) {
			job.setOutputFormatClass(TextOutputFormat.class);
		}
		else {
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TratzParsedDocument.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TratzParsedDocument.class);

		job.setMapperClass(MyMapper.class);

		job.setJarByClass(TratzParse.class);

		// no reducers needed
		job.setNumReduceTasks(0);

		// run job
		job.waitForCompletion(true);

		// print job statistics
		Counters counters = job.getCounters();
		sLogger.info(" - Total documents: " + counters.findCounter(StatCounters.TOTAL_DOCUMENTS).getValue());
		sLogger.info(" - Total sentences: " + counters.findCounter(StatCounters.TOTAL_SENTENCES).getValue());
		sLogger.info(" - Total tokens: " + counters.findCounter(StatCounters.TOTAL_TOKENS).getValue());
		sLogger.info(" - Total dropped sentences: " + counters.findCounter(StatCounters.TOTAL_DROPPED_SENTENCES).getValue());
		sLogger.info(" - Total tokenization time (ms): " + counters.findCounter(StatCounters.TOKENIZE_TIME).getValue());
		sLogger.info(" - Total POS tagging time (ms): " + counters.findCounter(StatCounters.POSTAG_TIME).getValue());
		sLogger.info(" - Total chunking time (ms): " + counters.findCounter(StatCounters.CHUNK_TIME).getValue());
		sLogger.info(" - Total named entity tagging time (ms): " + counters.findCounter(StatCounters.NETAG_TIME).getValue());
		sLogger.info(" - Total parse time (ms): " + counters.findCounter(StatCounters.PARSE_TIME).getValue());

		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new TratzParse(conf), args);
		System.exit(res);
	}

}
