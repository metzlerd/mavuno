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
import java.util.List;
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
import edu.isi.mavuno.nlp.NLProcTools;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;
import edu.stanford.nlp.ling.Word;
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.util.map.HMapKF;
import edu.umd.cloud9.util.map.HMapKL;
import edu.umd.cloud9.util.map.MapKF;

/**
 * @author metzler
 *
 */
public class HarvestUDAPInstances extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(HarvestUDAPInstances.class);

	private static final int MAX_ITERATIONS = 10;

	public HarvestUDAPInstances(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, Text, Text> {

		// text utility class
		private final NLProcTools mTextUtils = new NLProcTools();

		@Override
		public void setup(Mapper<Writable, Indexable, Text, Text>.Context context) throws IOException {
			try {
				// initialize WordNet (needed by POS tagger)
				mTextUtils.initializeWordNet();

				// initialize POS tagger
				mTextUtils.initializePOSTagger();

				// initialize named entity tagger
				mTextUtils.initializeNETagger();
			}
			catch(ClassNotFoundException e) {
				throw new RuntimeException(e);
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

			sLogger.info("Currently harvesting from document " + docId);

			// tokenize the document, strip any XML tags, and split into sentences
			List<List<Word>> sentences = mTextUtils.getTagStrippedSentences(text);

			// process each sentence
			for(List<Word> sentence : sentences) {
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

				// if so, then process sentence further...

				// skip very long sentences
				if(sentence.size() > NLProcTools.MAX_SENTENCE_LENGTH) {
					continue;
				}

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

				// assign chunk ids to each position within the sentence
				int [] chunkIds = NLProcTools.getChunkIds(tokens, neTags);

				// chunks should not contain the "such as" pattern
				int suchChunkId = chunkIds[matchPos];
				chunkIds[matchPos] = -1;

				int asChunkId = chunkIds[matchPos+1];
				chunkIds[matchPos+1] = -1;

				// get X chunks
				int xChunkId = suchChunkId - 1;
				Set<Text> xChunks = NLProcTools.extractChunks(xChunkId, chunkIds, tokens, neTags, true, true, false, false);

				// get Y chunks
				int yChunkId = asChunkId + 1;
				Set<Text> yChunks = NLProcTools.extractChunks(yChunkId, chunkIds, tokens, neTags, false, true, false, false);

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

		// maps from instances to frequency
		private final HMapKL<String> mInstanceFreq = new HMapKL<String>();

		// maps from lists of instances to frequency
		private final HMapKL<Text> mListFreq = new HMapKL<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
			// maps from instances to probabilities
			HMapKF<String> instanceProb = new HMapKF<String>();

			// maps from lists of instances to weights
			HMapKF<Text> listWeight = new HMapKF<Text>();

			mInstanceFreq.clear();
			mListFreq.clear();

			long totalInstances = 0;

			for(Text value : values) {
				String [] instances = value.toString().split(NLProcTools.SEPARATOR);
				for(String instance : instances) {
					mInstanceFreq.increment(instance);
					instanceProb.put(instance, 1);
				}

				mListFreq.increment(new Text(value));
				listWeight.put(new Text(value), 1);

				totalInstances += instances.length;
			}

			for(int iter = 0; iter < MAX_ITERATIONS; iter++) {
				// update list weights
				listWeight = updateListWeights(listWeight, instanceProb);

				// update instance probabilities
				instanceProb = updateInstanceProbs(listWeight, instanceProb);
			}

			// add total count to output
			instanceProb.put(ContextPatternWritable.ASTERISK_STRING, totalInstances);

			// sort the instances in descending order of their likelihood			
			for(MapKF.Entry<String> entry : instanceProb.getEntriesSortedByValue()) {
				String instance = entry.getKey();
				float prob = entry.getValue();
				mValue.set(prob);
				context.write(new Text(key + "\t" + instance), mValue);
			}
		}

		private HMapKF<Text> updateListWeights(HMapKF<Text> weights, HMapKF<String> probs) {
			HMapKF<Text> newWeights = new HMapKF<Text>();

			for(Text list : weights.keySet()) {
				float newWeight = 0;

				String [] instances = list.toString().split(NLProcTools.SEPARATOR);
				for(String instance : instances) {
					newWeight += probs.get(instance);
				}

				newWeights.put(list, newWeight);
			}

			return newWeights;
		}

		private HMapKF<String> updateInstanceProbs(HMapKF<Text> weights, HMapKF<String> probs) {
			HMapKF<String> newProbs = new HMapKF<String>();

			for(String instance : probs.keySet()) {
				long totalMatches = 0;
				long totalLists = 0;

				for(Text list : weights.keySet()) {
					boolean matches = false;

					String [] instances = list.toString().split(NLProcTools.SEPARATOR);
					for(String i : instances) {
						if(instance.equals(i)) {
							matches = true;
							break;
						}
					}

					long listFreq = mListFreq.get(list);

					if(matches) {
						totalMatches += listFreq;
					}

					totalLists += listFreq;
				}

				newProbs.put(instance, (float)totalMatches / (float)totalLists);
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
