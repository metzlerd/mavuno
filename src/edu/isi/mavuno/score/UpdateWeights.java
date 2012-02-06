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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class UpdateWeights extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(UpdateWeights.class);

	public UpdateWeights(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<ContextPatternWritable, Writable, ContextPatternWritable, ContextPatternStatsWritable> {

		private final ContextPatternStatsWritable mStats = new ContextPatternStatsWritable();

		@Override
		public void map(ContextPatternWritable key, Writable value, Mapper<ContextPatternWritable, Writable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			if(value instanceof DoubleWritable) {
				mStats.weight = ((DoubleWritable)value).get();
				context.write(key, mStats);
			}
			else {
				context.write(key, (ContextPatternStatsWritable)value);
			}
		}

	}

	private static class MyReducer extends Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {

		private double mWeight;

		private final Text mContext = new Text();
		private final Text mPattern = new Text();

		private boolean mPatternType = false;
		private boolean mContextType = false;

		@Override
		public void setup(Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) {
			Configuration conf = context.getConfiguration();

			mPatternType = false;
			mContextType = false;
			
			String exampleType = conf.get("Mavuno.UpdateWeights.ExampleType").toLowerCase();
			if("pattern".equals(exampleType)) {
				mPatternType = true;
			}
			else if("context".equals(exampleType)) {
				mContextType = true;
			}
			else {
				throw new RuntimeException("Invalid ExampleType in UpdateExampleWeight -- " + exampleType);
			}
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<ContextPatternStatsWritable> values, Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			if(mPatternType) {
				for(ContextPatternStatsWritable value : values) {
					if(key.getContext().equals(ContextPatternWritable.ASTERISK)) {
						mWeight = value.weight;
						mPattern.set(key.getPattern());
					}
					else if(key.getPattern().equals(mPattern)) {
						value.weight = mWeight;
						context.write(key, value);
					}
				}
			}
			else if(mContextType) {
				for(ContextPatternStatsWritable value : values) {
					if(key.getPattern().equals(ContextPatternWritable.ASTERISK)) {
						mWeight = value.weight;
						mContext.set(key.getContext());
					}
					else if(key.getContext().equals(mContext)) {
						value.weight = mWeight;
						context.write(key, value);
					}
				}				
			}
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.UpdateWeights", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String statsPath = MavunoUtils.getRequiredParam("Mavuno.UpdateWeights.StatsPath", conf);
		String scoresPath = MavunoUtils.getRequiredParam("Mavuno.UpdateWeights.ScoresPath", conf);
		String exampleType = MavunoUtils.getRequiredParam("Mavuno.UpdateWeights.ExampleType", conf).toLowerCase();
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.UpdateWeights.OutputPath", conf);

		sLogger.info("Tool name: UpdateWeights");
		sLogger.info(" - Stats path: " + statsPath);
		sLogger.info(" - Scores path: " + scoresPath);
		sLogger.info(" - Example type: " + exampleType);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("UpdateWeights");
		job.setJarByClass(UpdateWeights.class);

		FileInputFormat.addInputPath(job, new Path(statsPath));
		FileInputFormat.addInputPath(job, new Path(scoresPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(ContextPatternWritable.class);

		if("pattern".equals(exampleType)) {
			job.setSortComparatorClass(ContextPatternWritable.IdPatternComparator.class);
		}
		else if("context".equals(exampleType)) {
			job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		}
		else {
			throw new RuntimeException("Invalid ExampleType in UpdateExampleWeight -- " + exampleType);
		}

		job.setPartitionerClass(ContextPatternWritable.IdPartitioner.class);
		job.setMapOutputValueClass(ContextPatternStatsWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(ContextPatternStatsWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);		
		return 0;
	}

}
