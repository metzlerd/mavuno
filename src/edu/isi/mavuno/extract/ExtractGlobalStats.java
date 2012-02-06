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
import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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

import edu.isi.mavuno.extract.CombineGlobalStats;
import edu.isi.mavuno.extract.Extractor;
import edu.isi.mavuno.extract.Split;
import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;
import edu.umd.cloud9.util.map.HMapKL;

/**
 * @author metzler
 *
 */
public class ExtractGlobalStats extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ExtractGlobalStats.class);

	public ExtractGlobalStats(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable> {

		private Extractor mExtractor = null;

		private final ContextPatternWritable mPair = new ContextPatternWritable();

		private long mTotalTerms;

		private final ContextPatternWritable mContext = new ContextPatternWritable();
		private final ContextPatternStatsWritable mStats = new ContextPatternStatsWritable();
		private final HMapKL<Text> mExampleStats = new HMapKL<Text>();

		private boolean mPatternTarget = false;
		private boolean mContextTarget = false;

		private void loadExample(String examplesPath, Configuration conf) throws IOException {
			mExampleStats.clear();
			mTotalTerms = 0L;

			String exampleStr = null;
			final Text example = new Text();

			BufferedReader reader = MavunoUtils.getBufferedReader(conf, examplesPath);

			String input;
			while((input = reader.readLine()) != null) {
				String [] cols = input.split("\t");

				if(mPatternTarget) {
					exampleStr = cols[ContextPatternWritable.PATTERN_FIELD];
				}
				else if(mContextTarget) {
					exampleStr = cols[ContextPatternWritable.CONTEXT_FIELD];
				}

				example.set(exampleStr);
				if(!mExampleStats.containsKey(example)) {
					mExampleStats.put(new Text(example), 0);
				}
			}

			reader.close();
		}

		@Override
		public void setup(Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			try {
				// initialize extractor
				mExtractor = (Extractor)Class.forName(conf.get("Mavuno.ExtractGlobalStats.ExtractorClass")).newInstance();
				String contextArgs = conf.get("Mavuno.ExtractGlobalStats.ExtractorArgs", null);
				mExtractor.initialize(contextArgs, conf);

				// get extraction target
				String target = conf.get("Mavuno.ExtractGlobalStats.ExtractorTarget");
				if("pattern".equals(target)) {
					mPatternTarget = true;
				}
				else if("context".equals(target)) {
					mContextTarget = true;
				}
				else {
					throw new RuntimeException("Invalid ExtractorTarget in ExtractGlobalStats -- " + target);
				}

				// load examples we're matching into memory
				String examplesPath = conf.get("Mavuno.ExtractGlobalStats.ExamplesPath", null);
				loadExample(examplesPath, conf);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			// set current document
			mExtractor.setDocument(doc);

			// extract example counts
			while(mExtractor.getNextPair(mPair)) {
				if(mPatternTarget) {
					if(mExampleStats.containsKey(mPair.getPattern())) {
						mExampleStats.increment(mPair.getPattern());
					}
				}
				else if(mContextTarget) {
					if(mExampleStats.containsKey(mPair.getContext())) {
						mExampleStats.increment(mPair.getContext());
					}
				}

				// increment number of pairs
				mTotalTerms++;
			}
		}

		@Override
		public void cleanup(Mapper<Writable, Indexable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			String examplesPath = conf.get("Mavuno.ExtractGlobalStats.ExamplesPath", null);

			mContext.clear();
			mStats.clear();

			if(mPatternTarget) {
				mStats.globalPatternCount = mTotalTerms;
			}
			else if(mContextTarget) {
				mStats.globalContextCount = mTotalTerms;
			}

			context.write(mContext, mStats);

			BufferedReader reader = MavunoUtils.getBufferedReader(conf, examplesPath);

			long count = 0L;

			String input;
			while((input = reader.readLine()) != null) {
				String [] cols = input.split("\t");

				mContext.setId(cols[ContextPatternWritable.ID_FIELD]);
				mStats.weight = Double.parseDouble(cols[ContextPatternWritable.TOTAL_FIELDS]);

				if(mPatternTarget) {
					mContext.setPattern(cols[ContextPatternWritable.PATTERN_FIELD]);

					mStats.globalPatternCount = mExampleStats.get(mContext.getPattern());
					count = mStats.globalPatternCount;
				}
				else if(mContextTarget) {
					mContext.setContext(cols[ContextPatternWritable.CONTEXT_FIELD]);

					mStats.globalContextCount = mExampleStats.get(mContext.getContext());
					count = mStats.globalContextCount;
				}

				if(count > 0) {
					context.write(mContext, mStats);
				}
			}

			reader.close();
		}
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {
		private ContextPatternStatsWritable mResult = new ContextPatternStatsWritable();

		private String mTotalTermsPath = null;

		private boolean mPatternTarget = false;
		private boolean mContextTarget = false;

		@Override
		public void setup(Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			mTotalTermsPath = conf.get("Mavuno.TotalTermsPath", null);

			// get extraction target
			String target = conf.get("Mavuno.ExtractGlobalStats.ExtractorTarget").toLowerCase();
			if("pattern".equals(target)) {
				mPatternTarget = true;
			}
			else if("context".equals(target)) {
				mContextTarget = true;
			}
			else {
				throw new RuntimeException("Invalid ExtractorTarget in ExtractGlobalStats -- " + target);
			}
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<ContextPatternStatsWritable> values, Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			mResult.zero();

			for(ContextPatternStatsWritable c : values) {
				mResult.increment(c);
			}

			// write total terms
			if(key.getId().equals(ContextPatternWritable.ASTERISK)) {
				if(!MavunoUtils.pathExists(conf, mTotalTermsPath)) {
					BufferedWriter writer = MavunoUtils.getBufferedWriter(conf, mTotalTermsPath);
					if(mPatternTarget) {
						writer.write(Long.toString(mResult.globalPatternCount));
					}
					else if(mContextTarget) {
						writer.write(Long.toString(mResult.globalContextCount));					
					}
					writer.close();
				}
				return;
			}

			context.write(key, mResult);
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ExtractGlobalStats", getConf());
		return run();
	}
	
	@SuppressWarnings("unchecked")
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.ExtractGlobalStats.InputPath", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.ExtractGlobalStats.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.ExtractGlobalStats.CorpusClass", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.ExtractGlobalStats.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.ExtractGlobalStats.ExtractorArgs", conf);
		String extractorTarget = MavunoUtils.getRequiredParam("Mavuno.ExtractGlobalStats.ExtractorTarget", conf).toLowerCase();
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ExtractGlobalStats.OutputPath", conf);

		// split examples
		conf.set("Mavuno.Split.InputPath", inputPath);
		conf.set("Mavuno.Split.OutputPath", outputPath + "/../split");
		conf.set("Mavuno.Split.SplitKey", extractorTarget);
		new Split(conf).run();

		// get splits
		FileStatus [] files = MavunoUtils.getDirectoryListing(conf, outputPath + "/../split");
		int split = 0;
		for(FileStatus file : files) {
			if(!file.getPath().getName().endsWith(".examples")) {
				continue;
			}

			conf.set("Mavuno.ExtractGlobalStats.ExamplesPath", file.getPath().toString());			

			sLogger.info("Tool name: ExtractGlobalStats");
			sLogger.info(" - Input path: " + inputPath);
			sLogger.info(" - Examples path: " + file.getPath());
			sLogger.info(" - Example split: " + split);
			sLogger.info(" - Corpus path: " + corpusPath);
			sLogger.info(" - Corpus class: " + corpusClass);
			sLogger.info(" - Extractor class: " + extractorClass);
			sLogger.info(" - Extractor class: " + extractorArgs);
			sLogger.info(" - Extractor target: " + extractorTarget);
			sLogger.info(" - Output path: " + outputPath);

			Job job = new Job(conf);
			job.setJobName("ExtractGlobalStats");
			job.setJarByClass(ExtractGlobalStats.class);

			MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/../split/" + split));

			job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

			job.setMapOutputKeyClass(ContextPatternWritable.class);
			job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
			job.setPartitionerClass(ContextPatternWritable.FullPartitioner.class);
			job.setMapOutputValueClass(ContextPatternStatsWritable.class);

			job.setOutputKeyClass(ContextPatternWritable.class);
			job.setOutputValueClass(ContextPatternStatsWritable.class);

			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);

			job.waitForCompletion(true);		

			split++;
		}

		// combine splits
		conf.setInt("Mavuno.CombineGlobalStats.TotalSplits", split);
		conf.set("Mavuno.CombineGlobalStats.InputPath", outputPath + "/../split/");
		conf.set("Mavuno.CombineGlobalStats.OutputPath", outputPath);
		new CombineGlobalStats(conf).run();

		MavunoUtils.removeDirectory(conf, outputPath + "/../split");

		return 0;
	}

}
