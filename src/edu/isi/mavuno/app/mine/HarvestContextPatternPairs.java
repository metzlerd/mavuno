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

package edu.isi.mavuno.app.mine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.extract.Extractor;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;
import edu.umd.cloud9.collection.Indexable;

/**
 * @author metzler
 *
 */
public class HarvestContextPatternPairs extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(HarvestContextPatternPairs.class);

	public HarvestContextPatternPairs(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, ContextPatternWritable, LongWritable> {

		private Extractor mExtractor = null;

		private final ContextPatternWritable mKey = new ContextPatternWritable();
		private final LongWritable mValue = new LongWritable(1L);

		@Override
		public void setup(Mapper<Writable, Indexable, ContextPatternWritable, LongWritable>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			try {
				// initialize extractor
				mExtractor = (Extractor)Class.forName(conf.get("Mavuno.HarvestContextPatternPairs.ExtractorClass")).newInstance();
				String contextArgs = conf.get("Mavuno.HarvestContextPatternPairs.ExtractorArgs", null);
				mExtractor.initialize(contextArgs, conf);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, ContextPatternWritable, LongWritable>.Context context) throws IOException, InterruptedException {
			// initialize extractor with current document
			mExtractor.setDocument(doc);

			// extract pattern counts
			while(mExtractor.getNextPair(mKey)) {
				mKey.setId(doc.getDocid());
				context.write(mKey, mValue);
			}
		}		
	}

	private static class MyCombiner extends Reducer<ContextPatternWritable, LongWritable, ContextPatternWritable, LongWritable> {

		private final LongWritable mValue = new LongWritable();

		@Override
		public void reduce(ContextPatternWritable key, Iterable<LongWritable> values, Reducer<ContextPatternWritable, LongWritable, ContextPatternWritable, LongWritable>.Context context) throws IOException, InterruptedException {
			long count = 0;
			for(LongWritable v : values) {
				count += v.get();
			}

			mValue.set(count);
			context.write(key, mValue);
		}

	}

	private static class MyReducer extends Reducer<ContextPatternWritable, LongWritable, ContextPatternWritable, LongWritable> {

		private long mMinMatches = 1;
		private final LongWritable mValue = new LongWritable();

		@Override
		public void setup(Reducer<ContextPatternWritable, LongWritable, ContextPatternWritable, LongWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			mMinMatches = conf.getLong("Mavuno.HarvestContextPatternPairs.MinMatches", 1);
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<LongWritable> values, Reducer<ContextPatternWritable, LongWritable, ContextPatternWritable, LongWritable>.Context context) throws IOException, InterruptedException {
			long count = 0;
			for(LongWritable v : values) {
				count += v.get();
			}

			if(count >= mMinMatches) {
				mValue.set(count);
				context.write(key, mValue);
			}
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.HarvestContextPatternPairs", getConf());
		return run();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.HarvestContextPatternPairs.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.HarvestContextPatternPairs.CorpusClass", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.HarvestContextPatternPairs.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.HarvestContextPatternPairs.ExtractorArgs", conf);
		String minMatches = MavunoUtils.getRequiredParam("Mavuno.HarvestContextPatternPairs.MinMatches", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestContextPatternPairs.OutputPath", conf);

		sLogger.info("Tool name: HarvestContextPatternPairs");
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Extractor class: " + extractorClass);
		sLogger.info(" - Extractor args: " + extractorArgs);
		sLogger.info(" - Min matches: " + minMatches);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("HarvestContextPatternPairs");

		MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		job.setPartitionerClass(ContextPatternWritable.FullPartitioner.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
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
		int res = ToolRunner.run(new HarvestContextPatternPairs(conf), args);
		System.exit(res);
	}

}
