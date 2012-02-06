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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import edu.isi.mavuno.extract.Extractor;
import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.IdWeightPair;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class Extract extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(Extract.class);

	public Extract(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable> {

		private Extractor mExtractor = null;

		private final ContextPatternWritable mPair = new ContextPatternWritable();
		private final ContextPatternStatsWritable mStats = new ContextPatternStatsWritable(1L, 0L, 0L, 0.0, 0.0);

		private final Map<Text, List<IdWeightPair>> mExamples =  new HashMap<Text, List<IdWeightPair>>();

		private boolean mPatternTarget = false;
		private boolean mContextTarget = false;

		private void loadExamples(String examplesPath, Configuration conf) throws IOException {
			// clear example lookup
			mExamples.clear();

			BufferedReader reader = MavunoUtils.getBufferedReader(conf, examplesPath);

			Map<String, Text> idMap = new HashMap<String, Text>();
			Map<String, IdWeightPair> idWeightMap = new HashMap<String, IdWeightPair>();

			// read next batch of contexts from hdfs
			String input;
			while((input = reader.readLine()) != null) {
				String [] cols = input.split("\t");

				float weight = Float.parseFloat(cols[ContextPatternWritable.TOTAL_FIELDS]);

				Text id = idMap.get(cols[ContextPatternWritable.ID_FIELD]);
				if(id == null) {
					id = new Text(cols[ContextPatternWritable.ID_FIELD]);
					idMap.put(cols[ContextPatternWritable.ID_FIELD], id);
				}

				String idWeightStr = cols[ContextPatternWritable.ID_FIELD] + "\t" + cols[ContextPatternWritable.TOTAL_FIELDS];
				IdWeightPair pair = idWeightMap.get(idWeightStr);
				if(pair == null) {
					pair = new IdWeightPair(id, weight);
					idWeightMap.put(idWeightStr, pair);
				}

				Text key = null;
				if(mPatternTarget) {
					String context = cols[ContextPatternWritable.CONTEXT_FIELD];

					if(context.equals(ContextPatternWritable.ASTERISK_STRING)) {
						continue;
					}

					key = new Text(context);
				}
				else if(mContextTarget) {
					String pattern = cols[ContextPatternWritable.PATTERN_FIELD];

					if(pattern.equals(ContextPatternWritable.ASTERISK_STRING)) {
						continue;
					}

					key = new Text(pattern);
				}

				updateExamples(key, pair);
			}

			// close current reader
			reader.close();
		}

		private void updateExamples(Text key, IdWeightPair pair) {
			// populate context map
			List<IdWeightPair> contextList = null;
			contextList = mExamples.get(key);
			if(contextList == null) {
				contextList = new ArrayList<IdWeightPair>(1);
				contextList.add(pair);
				mExamples.put(key, contextList);
			}
			else {
				contextList.add(pair);
			}
		}

		private void outputDummyMatches(Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			final ContextPatternStatsWritable dummyStats = new ContextPatternStatsWritable(0L, 0L, 0L, 0.0, 0.0);
			for(Text key : mExamples.keySet()) {
				List<IdWeightPair> contextList = mExamples.get(key);
				for(IdWeightPair pair : contextList) {
					mPair.setId(pair.id);
					
					if(mPatternTarget) {
						mPair.setPattern(key);
					}
					else if(mContextTarget) {
						mPair.setContext(key);
					}

					dummyStats.weight = pair.weight;
					context.write(mPair, dummyStats);
				}
			}
		}
		
		@Override
		public void setup(Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			try {
				// initialize extractor
				mExtractor = (Extractor)Class.forName(conf.get("Mavuno.Extract.ExtractorClass")).newInstance();
				String contextArgs = conf.get("Mavuno.Extract.ExtractorArgs", null);
				mExtractor.initialize(contextArgs, conf);

				String target = conf.get("Mavuno.Extract.ExtractorTarget").toLowerCase();
				if("pattern".equals(target)) {
					mPatternTarget = true;
				}
				else if("context".equals(target)) {
					mContextTarget = true;
				}
				else {
					throw new RuntimeException("Invalid extractor target in ExtracExamples -- " + target);
				}

				// load target examples into memory
				String examplesPath = conf.get("Mavuno.Extract.InputPath", null);
				loadExamples(examplesPath, conf);
				
				// print "dummy" matches to ensure that all examples (not just those that match something)
				// show up in the remainder of the pipeline
				outputDummyMatches(context);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			// set current document
			mExtractor.setDocument(doc);

			// extract patterns
			List<IdWeightPair> contextList = null;
			while(mExtractor.getNextPair(mPair)) {
				if(mPatternTarget) {
					contextList = mExamples.get(mPair.getContext());
				}
				else if(mContextTarget) {
					contextList = mExamples.get(mPair.getPattern());
				}

				if(contextList == null) {
					continue;
				}

				// if found, then write stats to output
				for(IdWeightPair pair : contextList) {
					mStats.weight = pair.weight;

					mPair.setId(pair.id);
					context.write(mPair, mStats);

					if(mPatternTarget) {
						mPair.setPattern(ContextPatternWritable.ASTERISK);
					}
					else if(mContextTarget) {
						mPair.setContext(ContextPatternWritable.ASTERISK);
					}

					context.write(mPair, mStats);
				}
			}
		}
	}

	private static class MyCombiner extends Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {

		private final ContextPatternStatsWritable mResult = new ContextPatternStatsWritable();

		@Override
		public void reduce(ContextPatternWritable key, Iterable<ContextPatternStatsWritable> values, Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			mResult.zero();

			for(ContextPatternStatsWritable c : values) {
				mResult.increment(c);
			}

			context.write(key, mResult);
		}
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {

		private final ContextPatternStatsWritable mResult = new ContextPatternStatsWritable();
		
		private long mContextCount;
		private long mPatternCount;

		private int mMinMatches;

		private boolean mPatternTarget = false;
		private boolean mContextTarget = false;

		@Override
		public void setup(Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			mMinMatches = conf.getInt("Mavuno.Extract.MinMatches", 1);
			
			String target = conf.get("Mavuno.Extract.ExtractorTarget").toLowerCase();
			if("pattern".equals(target)) {
				mPatternTarget = true;
			}
			else if("context".equals(target)) {
				mContextTarget = true;
			}
			else {
				throw new RuntimeException("Invalid extractor target in ExtracExamples -- " + target);
			}
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<ContextPatternStatsWritable> values, Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			mResult.zero();
			
			for(ContextPatternStatsWritable c : values) {
				mResult.increment(c);
			}

			if(mContextTarget && key.getContext().equals(ContextPatternWritable.ASTERISK)) {
				mPatternCount = mResult.matches;
				return;
			}

			if(mPatternTarget && key.getPattern().equals(ContextPatternWritable.ASTERISK)) {
				mContextCount = mResult.matches;
				return;
			}

			if(mResult.matches >= mMinMatches) {
				if(mContextTarget) {
					mResult.globalPatternCount = mPatternCount;
				}
				if(mPatternTarget) {
					mResult.globalContextCount = mContextCount;
				}
				
				context.write(key, mResult);
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.Extract", getConf());
		return run();
	}
	
	@SuppressWarnings("unchecked")
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.Extract.InputPath", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.Extract.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.Extract.CorpusClass", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.Extract.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.Extract.ExtractorArgs", conf);
		String extractorTarget = MavunoUtils.getRequiredParam("Mavuno.Extract.ExtractorTarget", conf).toLowerCase();
		int minContextMatches = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.Extract.MinMatches", conf));
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.Extract.OutputPath", conf);

		sLogger.info("Tool name: Extract");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Extractor class: " + extractorClass);
		sLogger.info(" - Extractor arguments: " + extractorArgs);
		sLogger.info(" - Extractor target: " + extractorTarget);
		sLogger.info(" - Min context matches: " + minContextMatches);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("Extract");
		job.setJarByClass(Extract.class);

		MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		if("pattern".equals(extractorTarget)) {
			job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
			job.setPartitionerClass(ContextPatternWritable.IdContextPartitioner.class);
		}
		else if("context".equals(extractorTarget)) {
			job.setSortComparatorClass(ContextPatternWritable.IdPatternComparator.class);
			job.setPartitionerClass(ContextPatternWritable.IdPatternPartitioner.class);
		}
		else {
			throw new RuntimeException("Invalid extractor target in Extract -- " + extractorTarget);
		}

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setMapOutputValueClass(ContextPatternStatsWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(ContextPatternStatsWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);
		return 0;
	}

}
