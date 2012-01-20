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

package edu.isi.mavuno.app.ie;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tratz.parse.types.Token;
import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.nlp.NLProcTools;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.TratzParsedTokenWritable;
import edu.stanford.nlp.ling.Word;
import edu.umd.cloud9.util.map.HMapKF;
import edu.umd.cloud9.util.map.HMapKL;
import edu.umd.cloud9.util.map.MapKF;

/**
 * @author metzler
 *
 */
public class HarvestUDAPInstances extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(HarvestUDAPInstances.class);

	private static enum MyCounters {
		MATCHED_SENTENCES,
		TOTAL_SENTENCES
	}

	// TODO: make these configurable
	private static final float LAMBDA = 0.7f;
	private static final int MAX_ITERATIONS = 25;
	
	public HarvestUDAPInstances(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, Text, Text> {

		// text utility class
		private final NLProcTools mTextUtils = new NLProcTools();

		@Override
		public void setup(Mapper<Writable, Indexable, Text, Text>.Context context) throws IOException {
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

			// initialize named entity tagger
			try {
				mTextUtils.initializeNETagger();
			}
			catch(Exception  e) {
				throw new RuntimeException("Error initializing named entity tagger -- " + e);
			}
		}

		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, Text, Text>.Context context) throws IOException, InterruptedException {
			// get the document id
			String docId = doc.getDocid();

			// get the document content
			String text = doc.getContent();

			// require both a document id and some non-null content
			if(docId == null || text == null) {
				return;
			}

			// tokenize the document, strip any XML tags, and split into sentences
			List<List<Word>> sentences = mTextUtils.getTagStrippedSentences(text);

			// process each sentence
			for(List<Word> sentence : sentences) {
				// bookkeeping
				context.getCounter(MyCounters.TOTAL_SENTENCES).increment(1L);

				// check if this sentence contains the pattern "X such as Y"
				boolean matches = false;
				int matchPos = -1;
				for(int i = 0; i < sentence.size() - 1; i++) {
					Word cur = sentence.get(i);
					Word next = sentence.get(i+1);
					if(cur.word().equals("such") && next.word().equals("as")) {
						matches = true;
						matchPos = i;
						break;
					}
				}

				// if not, then go onto the next sentence
				if(matchPos <= 0 || matchPos + 2 >= sentence.size() || !matches) {
					continue;
				}

				// skip very long sentences
				if(sentence.size() > NLProcTools.MAX_SENTENCE_LENGTH) {
					continue;
				}

				// bookkeeping
				context.getCounter(MyCounters.MATCHED_SENTENCES).increment(1L);

				// set the sentence to be processed
				mTextUtils.setSentence(sentence);

				// get sentence tokens
				List<Token> tokens = mTextUtils.getSentenceTokens();

				// get part of speech tags
				List<String> posTags = mTextUtils.getPosTags();

				// get named entity tags
				List<String> neTags = mTextUtils.getNETags();

				// lowercase the first term in the sentence if it's not a proper noun
				if(!posTags.get(0).startsWith("NNP")) {
					tokens.get(0).setText(tokens.get(0).getText().toLowerCase());
				}

				// created tagged tokens
				List<TratzParsedTokenWritable> taggedTokens = NLProcTools.getTaggedTokens(tokens, posTags, neTags);
				
				// assign chunk ids to each position within the sentence
				int [] chunkIds = NLProcTools.getChunkIds(taggedTokens, true);

				// chunks should not contain the "such as" pattern
				int suchChunkId = chunkIds[matchPos];
				chunkIds[matchPos] = -1;

				int asChunkId = chunkIds[matchPos+1];
				chunkIds[matchPos+1] = -1;

				// get X chunks
				int xChunkId = suchChunkId - 1;
				Set<Text> xChunks = NLProcTools.extractChunks(xChunkId, chunkIds, taggedTokens, true, true, false, false);

				// get Y chunks
				int yChunkId = asChunkId + 1;
				Set<Text> yChunks = NLProcTools.extractChunks(yChunkId, chunkIds, taggedTokens, false, true, false, false);

				// emit cross product of X and Y chunks
				for(Text xChunk : xChunks) {
					for(Text yChunk : yChunks) {
						context.write(xChunk, yChunk);
					}
				}

			}
		}
	}

	private static class MyReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		private final DoubleWritable mValue = new DoubleWritable();

		// maps from lists of instances to frequency
		private final HMapKL<Text> mListFreq = new HMapKL<Text>();

		// maps from instances to frequency
		private final HMapKL<String> mInstancesFreq = new HMapKL<String>();

		// maps from lists of instances to sets of instances
		private final Map<Text,Set<String>> mListInstances = new HashMap<Text,Set<String>>();

		// maps from instances to sets of lists of instances
		private final Map<String,Set<Text>> mInstancesList = new HashMap<String,Set<Text>>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
			// maps from instances to probabilities
			HMapKF<String> instanceProbs = new HMapKF<String>();

			// maps from lists of instances to proabilities
			HMapKF<Text> listProbs = new HMapKF<Text>();

			mListFreq.clear();
			mInstancesFreq.clear();
			
			mListInstances.clear();
			mInstancesList.clear();

			long totalInstances = 0;

			for(Text value : values) {
				
				Text list = new Text(value);
				
				Set<String> instanceSet = mListInstances.get(value);
				if(instanceSet == null) {
					instanceSet = new HashSet<String>();
					mListInstances.put(list, instanceSet);
				}
				
				// split list into individual instances
				String [] instances = value.toString().split(NLProcTools.SEPARATOR);
				
				for(String instance : instances) {
					instanceSet.add(instance);
					
					Set<Text> listSet = mInstancesList.get(instance);
					if(listSet == null) {
						listSet = new HashSet<Text>();
						mInstancesList.put(instance, listSet);
					}
					listSet.add(list);

					mInstancesFreq.increment(instance);
					instanceProbs.put(instance, 1);
				}

				mListFreq.increment(list);
				listProbs.put(list, instances.length);

				totalInstances += instances.length;
			}

			// normalize probabilities
			normalizeProbs(listProbs);
			normalizeProbs(instanceProbs);
			
			for(int iter = 0; iter < MAX_ITERATIONS; iter++) {
				// update list weights
				listProbs = updateListProbs(listProbs, instanceProbs, context);

				// update instance probabilities
				instanceProbs = updateInstanceProbs(listProbs, instanceProbs, context);				
			}

			// add total count to output
			instanceProbs.put(ContextPatternWritable.ASTERISK_STRING, totalInstances);

			// sort the instances in descending order of their likelihood			
			for(MapKF.Entry<String> entry : instanceProbs.getEntriesSortedByValue()) {
				String instance = entry.getKey();
				float prob = entry.getValue();
				mValue.set(prob);
				context.write(new Text(key + "\t" + instance), mValue);
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private void normalizeProbs(HMapKF probs) {
			Set<Comparable> keys = probs.keySet();

			float total = 0;
			for(Comparable key : keys) {
				total += probs.get(key);
			}
			
			for(Comparable key : keys) {
				probs.put(key, probs.get(key) / total);
			}			
		}
		
		private HMapKF<Text> updateListProbs(HMapKF<Text> listProbs, HMapKF<String> instanceProbs, Reducer<Text, Text, Text, DoubleWritable>.Context context) {
			HMapKF<Text> newProbs = new HMapKF<Text>();

			// the set of lists
			Set<Text> lists = listProbs.keySet();
			
			// static weight ("random surfer")
			float staticWeight = (1-LAMBDA) * (1.0f / lists.size());
			
			for(Text list : lists) {
				float newWeight = 0;

				// dynamic weight
				for(String instance : mListInstances.get(list)) {
					newWeight += instanceProbs.get(instance) * mListFreq.get(list) / mInstancesFreq.get(instance);
				}

				// update probability
				newProbs.put(list, staticWeight + LAMBDA * newWeight);

				// let hadoop know we've made some progress
				context.progress();
			}

			return newProbs;
		}

		private HMapKF<String> updateInstanceProbs(HMapKF<Text> listProbs, HMapKF<String> instanceProbs, Reducer<Text, Text, Text, DoubleWritable>.Context context) {
			HMapKF<String> newProbs = new HMapKF<String>();

			// the set of instances
			Set<String> instances = instanceProbs.keySet();
			
			// static weight ("random surfer")
			float staticWeight = (1-LAMBDA) * (1.0f / instances.size());
			
			for(String instance : instances) {
				float newWeight = 0;

				// dynamic weight
				for(Text list : mInstancesList.get(instance)) {
					newWeight += listProbs.get(list) * (1.0 / mListInstances.get(list).size());
				}

				// update probability
				newProbs.put(instance, staticWeight + LAMBDA * newWeight);

				// let hadoop know we've made some progress
				context.progress();
			}

			return newProbs;
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.HarvestUDAPInstances", getConf());
		return run();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.HarvestUDAPInstances.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.HarvestUDAPInstances.CorpusClass", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestUDAPInstances.OutputPath", conf);

		sLogger.info("Tool name: HarvestUDAPInstances");
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("HarvestUDAPInstances");

		MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new HarvestUDAPInstances(conf), args);
		System.exit(res);
	}

}
