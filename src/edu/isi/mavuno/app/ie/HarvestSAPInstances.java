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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.TratzParsedTokenWritable;
import edu.stanford.nlp.ling.Word;

/**
 * @author metzler
 *
 */
@SuppressWarnings("unused")
public class HarvestSAPInstances extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(HarvestSAPInstances.class);

	public HarvestSAPInstances(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, Text, LongWritable> {

		// text utility class
		private final NLProcTools mTextUtils = new NLProcTools();

		private final Text mKey = new Text();
		private final LongWritable mValue = new LongWritable(1L);

		@Override
		public void setup(Mapper<Writable, Indexable, Text, LongWritable>.Context context) throws IOException {
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
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
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
				Set<Text> xChunks = NLProcTools.extractChunks(xChunkId, chunkIds, taggedTokens, true, true, true, true);

				// get Y chunks
				int yChunkId = asChunkId + 1;
				Set<Text> yChunks = NLProcTools.extractChunks(yChunkId, chunkIds, taggedTokens, true, true, true, true);

				// emit cross product of X and Y chunks
				for(Text xChunk : xChunks) {
					for(Text yChunk : yChunks) {
						mKey.clear();
						mKey.append(xChunk.getBytes(), 0, xChunk.getLength());
						mKey.append(MavunoUtils.TAB_BYTES, 0, MavunoUtils.TAB_BYTES_LENGTH);
						mKey.append(yChunk.getBytes(), 0, yChunk.getLength());
						context.write(mKey, mValue);
					}
				}

			}
		}
	}

	private static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private long mMinMatches = 1;
		private final LongWritable mValue = new LongWritable();

		@Override
		public void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			mMinMatches = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.HarvestSAPInstances.MinMatches", conf));
		}

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long c = 0;
			for(LongWritable value : values) {
				c += value.get();
			}

			if(c >= mMinMatches) {
				mValue.set(c);
				context.write(key, mValue);
			}
		}

	}

	private static class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

		private final LongWritable mValue = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long c = 0;
			for(LongWritable value : values) {
				c += value.get();
			}

			mValue.set(c);
			context.write(key, mValue);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.HarvestSAPInstances", getConf());
		return run();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.HarvestSAPInstances.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.HarvestSAPInstances.CorpusClass", conf);
		int minMatches = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.HarvestSAPInstances.MinMatches", conf));
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestSAPInstances.OutputPath", conf);

		sLogger.info("Tool name: HarvestSAPInstances");
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Minimum matches: " + minMatches);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("HarvestSAPInstances");

		MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
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
		int res = ToolRunner.run(new HarvestSAPInstances(conf), args);
		System.exit(res);
	}

}
