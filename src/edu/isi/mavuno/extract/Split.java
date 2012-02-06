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

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class Split extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(Split.class);

	public Split(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<ContextPatternWritable, Writable, ContextPatternWritable, DoubleWritable> {

		private final ContextPatternWritable mContext = new ContextPatternWritable();
		private final DoubleWritable mResult = new DoubleWritable();

		private boolean mIncludePattern = true;
		private boolean mIncludeContext = true;

		@Override
		public void setup(Mapper<ContextPatternWritable, Writable, ContextPatternWritable, DoubleWritable>.Context context) {
			Configuration conf = context.getConfiguration();
			String splitKey = conf.get("Mavuno.Split.SplitKey").toLowerCase();
			if("pattern".equals(splitKey)) { // split on patterns
				mIncludePattern = true;
				mIncludeContext = false;
			}
			else if("context".equals(splitKey)) { // split on contexts
				mIncludePattern = false;
				mIncludeContext = true;
			}
			else if("pattern+context".equals(splitKey)) { // split on (pattern, context) pairs
				mIncludePattern = true;
				mIncludeContext = true;
			}
			else {
				throw new RuntimeException("Illegal SplitKey used in Split -- " + splitKey);
			}
		}

		@Override
		public void map(ContextPatternWritable key, Writable value, Mapper<ContextPatternWritable, Writable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			// construct context
			mContext.setId(key.getId());

			if(mIncludePattern) {
				mContext.setPattern(key.getPattern());
			}

			if(mIncludeContext) {
				mContext.setContext(key.getContext());
			}

			if(value instanceof DoubleWritable) {
				mResult.set(((DoubleWritable)value).get());
			}
			else if(value instanceof ContextPatternStatsWritable) {
				mResult.set(((ContextPatternStatsWritable)value).weight);				
			}

			// output result
			context.write(mContext, mResult);
		}
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable> {

		private static final int MAX_BYTES_PER_SPLIT = 200 * 1024 * 1024;

		private String mOutputPath = null;

		private BufferedWriter mPatternSplitWriter = null;

		private int mBytesProcessed;
		private int mCurrentSplit;

		@Override
		public void setup(Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			mOutputPath = conf.get("Mavuno.Split.OutputPath", null);

			mBytesProcessed = 0;
			mCurrentSplit = 0;

			mPatternSplitWriter = MavunoUtils.getBufferedWriter(conf, mOutputPath + "/" + mCurrentSplit + ".examples");
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<DoubleWritable> values, Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			if(mBytesProcessed >= MAX_BYTES_PER_SPLIT) {
				if(mPatternSplitWriter != null) {
					mPatternSplitWriter.close();
				}

				mBytesProcessed = 0;
				mCurrentSplit++;

				mPatternSplitWriter = MavunoUtils.getBufferedWriter(conf, mOutputPath + "/" + mCurrentSplit + ".examples");				
			}

			double sum = 0.0;
			for(DoubleWritable value : values) {
				sum += value.get();
			}

			mPatternSplitWriter.write(key.toString());
			mPatternSplitWriter.write('\t');
			mPatternSplitWriter.write(Double.toString(sum));
			mPatternSplitWriter.write('\n');

			mBytesProcessed += key.getLength();
		}

		@Override
		public void cleanup(Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			if(mPatternSplitWriter != null) {
				mPatternSplitWriter.close();
			}			
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.Split", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.Split.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.Split.OutputPath", conf);
		String splitKey = MavunoUtils.getRequiredParam("Mavuno.Split.SplitKey", conf);

		sLogger.info("Tool name: Split");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Split key: " + splitKey);

		Job job = new Job(conf);
		job.setJobName("Split");
		job.setJarByClass(Split.class);

		MavunoUtils.recursivelyAddInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		job.setPartitionerClass(ContextPatternWritable.FullPartitioner.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setNumReduceTasks(1);

		job.waitForCompletion(true);		
		return 0;
	}

}
