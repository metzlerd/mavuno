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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import opennlp.tools.chunker.Chunker;
import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;

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

import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.input.StanfordParsedDocument;
import edu.isi.mavuno.nlp.NLProcTools;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.StanfordParsedTokenWritable;
import edu.isi.mavuno.util.TokenFactory;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.IndexAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefClusterIdAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.trees.TreebankLanguagePack;
import edu.stanford.nlp.trees.TypedDependency;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Filter;

/**
 * @author metzler
 *
 */
public class ProcessStanfordNLP extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(ProcessStanfordNLP.class);

	private static final TokenFactory<StanfordParsedTokenWritable> TOKEN_FACTORY = new StanfordParsedTokenWritable.ExtendedParsedTokenFactory();

	private static enum MyCounters {
		TOTAL_DOCUMENTS,
		TOTAL_SENTENCES,
		TOTAL_TOKENS
	}

	public ProcessStanfordNLP(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, Text, StanfordParsedDocument> {

		// pipeline options
		private static final Properties PROPS = new Properties();

		static {
			PROPS.put("annotators", "tokenize, cleanxml, ssplit, pos, lemma, ner, parse, dcoref");

			PROPS.put("clean.xmltags", ".*");
			PROPS.put("clean.sentenceendingtags", "p");
			PROPS.put("clean.allowflawedxml", "true");
			PROPS.put("pos.maxlen", String.valueOf(NLProcTools.MAX_SENTENCE_LENGTH));
			PROPS.put("parser.maxlen", String.valueOf(NLProcTools.MAX_SENTENCE_LENGTH));
			
			PROPS.put("ner.useSUTime", "false");
		}

		// create pipeline
		private final StanfordCoreNLP mPipeline = new StanfordCoreNLP(PROPS);

		// document id
		private final Text mKey = new Text();

		// parsed document
		private final StanfordParsedDocument mDoc = new StanfordParsedDocument();

		// used for extracting information from parse trees
		private GrammaticalStructureFactory mGsf = null;

		// chunker
		private Chunker mChunker = null;

		// chnker words and POS tags
		private final List<String> mChunkerWords = new ArrayList<String>();
		private final List<String> mChunkerPosTags = new ArrayList<String>();

		@Override
		public void setup(Mapper<Writable, Indexable, Text, StanfordParsedDocument>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			
			TreebankLanguagePack tlp = new PennTreebankLanguagePack();
			Filter<String> puncWordFilter = tlp.punctuationWordRejectFilter();
			mGsf = tlp.grammaticalStructureFactory(puncWordFilter);

			// enable/disable SUTime (should be disabled for docs w/o date tags)
			String useSUTime = MavunoUtils.getOptionalParam("Mavuno.ProcessStanfordNLP.UseSUTime", conf);
			if(useSUTime != null) {
				PROPS.put("ner.useSUTime", useSUTime);
			}
			
			// initialize chunker
			mChunker = new ChunkerME(new ChunkerModel(getClass().getClassLoader().getResourceAsStream(NLProcTools.DEFAULT_CHUNKER_MODEL)));
		}

		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, Text, StanfordParsedDocument>.Context context) throws IOException, InterruptedException {
			// get the document id
			String docId = doc.getDocid();

			// get the document content
			String text = doc.getContent();

			// require both a document id and some non-null content
			if(docId == null || text == null) {
				return;
			}

			// initialize document
			mDoc.clear();

			// set document id
			mDoc.setDocId(docId);

			// create an empty Annotation just with the given text
			Annotation document = new Annotation(text);

			// run all Annotators on this text
			mPipeline.annotate(document);
			
			// these are all the sentences in this document
			// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
			List<CoreMap> sentences = document.get(SentencesAnnotation.class);

			for(CoreMap sentence: sentences) {
				// new parsed sentence
				SentenceWritable<StanfordParsedTokenWritable> parsedSentence = new SentenceWritable<StanfordParsedTokenWritable>(TOKEN_FACTORY);

				mChunkerWords.clear();
				mChunkerPosTags.clear();

				for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
					// parsed token
					StanfordParsedTokenWritable tok = new StanfordParsedTokenWritable();

					// token text
					String word = token.get(TextAnnotation.class);

					// char offsets
					int begin = Integer.parseInt(token.get(CharacterOffsetBeginAnnotation.class).toString());
					int end = Integer.parseInt(token.get(CharacterOffsetEndAnnotation.class).toString());

					// lemma
					String lemma = token.get(LemmaAnnotation.class);

					// POS tag
					String pos = token.get(PartOfSpeechAnnotation.class);

					// used later for chunking
					mChunkerWords.add(word);
					mChunkerPosTags.add(pos);

					// NER label
					String ne = token.get(NamedEntityTagAnnotation.class);       

					// coref cluster id
					Integer corefId = token.get(CorefClusterIdAnnotation.class);
					if(corefId == null) {
						corefId = -1;
					}

					// construct token
					tok.setToken(word);
					tok.setCharOffset(begin, end-1);
					tok.setLemma(lemma);
					tok.setPosTag(pos); 
					tok.setNETag(ne);
					tok.setCorefId(corefId);

					// add token to sentence
					parsedSentence.addToken(tok);

					// bookkeeping
					context.getCounter(MyCounters.TOTAL_TOKENS).increment(1L);
				}

				// sentence chunks
				List<String> chunkTags = Arrays.asList(mChunker.chunk(mChunkerWords.toArray(new String[mChunkerWords.size()]), mChunkerPosTags.toArray(new String[mChunkerPosTags.size()])));
				for(int i = 0; i < chunkTags.size(); i++) {
					parsedSentence.getTokenAt(i).setChunkTag(chunkTags.get(i));
				}

				// parse tree
				Tree tree = sentence.get(TreeAnnotation.class);

				// dependencies
				Collection<TypedDependency> dependencies = mGsf.newGrammaticalStructure(tree).typedDependencies();
				for(TypedDependency d : dependencies) {
					int headIndex = d.gov().label().get(IndexAnnotation.class);
					int dependentIndex = d.dep().label().get(IndexAnnotation.class);
					String dependType = d.reln().getShortName();
					if(dependType.length() == 0) {
						parsedSentence.getTokenAt(dependentIndex-1).setDependIndex(0);
						parsedSentence.getTokenAt(dependentIndex-1).setDependType("root");						
					}
					else {
						parsedSentence.getTokenAt(dependentIndex-1).setDependIndex(headIndex);
						parsedSentence.getTokenAt(dependentIndex-1).setDependType(dependType);
					}
				}

				// add sentence to document
				mDoc.addSentence(parsedSentence);

				// bookkeeping
				context.getCounter(MyCounters.TOTAL_SENTENCES).increment(1L);

				// let hadoop know we're making progress to avoid a timeout
				context.progress();
			}

			// set key (= doc id)
			mKey.set(docId);
			context.write(mKey, mDoc);
			context.getCounter(MyCounters.TOTAL_DOCUMENTS).increment(1L);
		}

		@Override
		public void cleanup(Mapper<Writable, Indexable, Text, StanfordParsedDocument>.Context context) throws IOException {
			sLogger.info("Pipeline timing information:\n" + mPipeline.timingInformation());
		}
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ProcessStanfordNLP", getConf());
		return run();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		// required parameters
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.ProcessStanfordNLP.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.ProcessStanfordNLP.CorpusClass", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ProcessStanfordNLP.OutputPath", conf);

		// optional parameters
		String suTime = MavunoUtils.getOptionalParam("Mavuno.ProcessStanfordNLP.UseSUTime", conf);
		String textOutput = MavunoUtils.getOptionalParam("Mavuno.ProcessStanfordNLP.TextOutputFormat", conf);

		sLogger.info("Tool name: ProcessStanfordNLP");
		sLogger.info(" - Input path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Output path: " + outputPath);

		if(suTime != null && Boolean.parseBoolean(suTime)) {
			sLogger.info("- SUTime enabled");
		}

		boolean textOutputFormat = false;
		if(textOutput != null && Boolean.parseBoolean(textOutput)) {
			sLogger.info("- Text output format enabled");
			textOutputFormat = true;
		}

		Job job = new Job(conf);
		job.setJobName("ProcessStanfordNLP");

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
		job.setMapOutputValueClass(StanfordParsedDocument.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StanfordParsedDocument.class);

		job.setMapperClass(MyMapper.class);

		job.setJarByClass(ProcessStanfordNLP.class);

		// no reducers needed
		job.setNumReduceTasks(0);

		// run job
		job.waitForCompletion(true);

		// print job statistics
		Counters counters = job.getCounters();
		sLogger.info(" - Total documents: " + counters.findCounter(MyCounters.TOTAL_DOCUMENTS).getValue());
		sLogger.info(" - Total sentences: " + counters.findCounter(MyCounters.TOTAL_SENTENCES).getValue());
		sLogger.info(" - Total tokens: " + counters.findCounter(MyCounters.TOTAL_TOKENS).getValue());

		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new ProcessStanfordNLP(conf), args);
		System.exit(res);
	}

}
