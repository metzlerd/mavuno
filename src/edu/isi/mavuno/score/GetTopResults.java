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

package edu.isi.mavuno.score;

import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.ContextPatternWritableScorePair;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class GetTopResults extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(GetTopResults.class);

	public GetTopResults(Configuration conf) throws IOException {
		super(conf);
	}

	private static class MyMapper extends Mapper<LongWritable, Text, ContextPatternWritable, DoubleWritable> {
		
		private final ContextPatternWritable mContext = new ContextPatternWritable();
		private final DoubleWritable mScore = new DoubleWritable();
		
		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			String [] cols = value.toString().split("\t");
			
			mContext.setId(cols[ContextPatternWritable.ID_FIELD]);
			mContext.setContext(cols[ContextPatternWritable.CONTEXT_FIELD]);
			mContext.setPattern(cols[ContextPatternWritable.PATTERN_FIELD]);
			
			mScore.set(Double.parseDouble(cols[ContextPatternWritable.TOTAL_FIELDS]));
			
			context.write(mContext, mScore);
		}
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable> {

		// number of results to return per id
		private int mNumResults;
		
		// priority queue to hold sorted results
		private final PriorityQueue<ContextPatternWritableScorePair> mTopResults = new PriorityQueue<ContextPatternWritableScorePair>();
		
		// current id
		private final Text mCurId = new Text();
		
		@Override
		public void setup(Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			try {
				// get maximum number of results requested
				mNumResults = Integer.parseInt(conf.get("Mavuno.GetTopResults.NumResults"));
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<DoubleWritable> values, Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {

			Text id = key.getId();
			
			if(!id.equals(mCurId)) {
				if(mCurId.getLength() > 0) {
					outputResults(context);
				}
				
				mTopResults.clear();
				mCurId.set(id);
			}
			
			double totalScore = 0.0;
			for(DoubleWritable value : values) {
				totalScore += value.get();
			}
			
			if(mTopResults.size() == mNumResults) {
				ContextPatternWritableScorePair pair = mTopResults.peek();
				if(pair.score.get() < totalScore) {
					pair = mTopResults.poll();
					pair.contextpattern.set(key);
					pair.score.set(totalScore);
					mTopResults.add(pair);					
				}
			}
			else {
				mTopResults.add(new ContextPatternWritableScorePair(key, totalScore));
			}
		}

		@Override
		public void cleanup(Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			if(mCurId.getLength() > 0) {
				outputResults(context);
			}			
		}
		
		private void outputResults(Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			Object [] results = mTopResults.toArray();
			Arrays.sort(results);
			for(int i = results.length - 1; i >= 0; i--) {
				ContextPatternWritableScorePair result = (ContextPatternWritableScorePair)results[i];
				context.write(result.contextpattern, result.score);				
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.GetTopResults", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.GetTopResults.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.GetTopResults.OutputPath", conf);
		int numResults = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.GetTopResults.NumResults", conf));
		boolean sequenceFileOutputFormat = conf.getBoolean("Mavuno.GetTopResults.SequenceFileOutputFormat", false);

		sLogger.info("Tool name: GetTopResults");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Number of results: " + numResults);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("GetTopResults");
		job.setJarByClass(GetTopResults.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		job.setPartitionerClass(ContextPatternWritable.IdPartitioner.class);

		if(sequenceFileOutputFormat) {
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		}
		else {
			job.setOutputFormatClass(TextOutputFormat.class);
		}

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
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
		int res = ToolRunner.run(new GetTopResults(conf), args);
		System.exit(res);
	}

}