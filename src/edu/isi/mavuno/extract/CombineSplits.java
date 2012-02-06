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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
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
public class CombineSplits extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(CombineSplits.class);

	public CombineSplits(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {
		/* identity mapper */
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {

		private final ContextPatternStatsWritable mContextStats = new ContextPatternStatsWritable();

		private boolean mPatternSplits = false;
		private boolean mContextSplits = false;
		private boolean mFullSplits = false;

		@Override
		public void setup(Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) {
			Configuration conf = context.getConfiguration();

			mPatternSplits = false;
			mContextSplits = false;
			mFullSplits = false;

			String splitKey = conf.get("Mavuno.CombineSplits.SplitKey");
			if("pattern".equals(splitKey)) {
				mPatternSplits = true;
			}
			else if("context".equals(splitKey)) {
				mContextSplits = true;
			}
			else if("pattern+context".equals(splitKey)) {
				mFullSplits = true;
			}
			else {
				throw new RuntimeException("Invalid SplitKey in CombineSplits! -- " + splitKey);
			}
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<ContextPatternStatsWritable> values, Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			long totalMatchCount = 0L;
			long totalPatternCount = 0L;
			long totalContextCount = 0L;

			double totalWeight = 0.0;

			for(ContextPatternStatsWritable p : values) {
				//System.out.println(key + "\t" + p);
				
				totalMatchCount += p.matches;
				totalPatternCount += p.globalPatternCount;
				totalContextCount += p.globalContextCount;
				totalWeight = p.weight;
			}

			mContextStats.matches = totalMatchCount;
			mContextStats.weight = totalWeight;

			if(mPatternSplits) {
				mContextStats.globalPatternCount = totalPatternCount;

				if(key.getPattern().equals(ContextPatternWritable.ASTERISK)) {
					mContextStats.globalContextCount = totalContextCount;
					return;
				}
			}
			else if(mContextSplits) {
				mContextStats.globalContextCount = totalContextCount;

				if(key.getContext().equals(ContextPatternWritable.ASTERISK)) {
					mContextStats.globalPatternCount = totalPatternCount;
					return;
				}
			}
			else if(mFullSplits) {
				mContextStats.globalPatternCount = totalPatternCount;
				mContextStats.globalContextCount = totalContextCount;
			}

			context.write(key, mContextStats);
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.CombineSplits", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String examplesPath = MavunoUtils.getRequiredParam("Mavuno.CombineSplits.ExamplesPath", conf);
		String exampleStatsPath = MavunoUtils.getRequiredParam("Mavuno.CombineSplits.ExampleStatsPath", conf);
		String splitKey = MavunoUtils.getRequiredParam("Mavuno.CombineSplits.SplitKey", conf).toLowerCase();
		int numSplits = conf.getInt("Mavuno.CombineSplits.TotalSplits", 1);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.CombineSplits.OutputPath", conf);

		sLogger.info("Tool name: CombineSplits");
		sLogger.info(" - Examples path: " + examplesPath);
		sLogger.info(" - Example stats path: " + exampleStatsPath);
		sLogger.info(" - Split key: " + splitKey);
		sLogger.info(" - Total splits: " + numSplits);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("CombineSplits");
		job.setJarByClass(CombineSplits.class);

		for(int split = 0; split < numSplits; split++) {
			FileInputFormat.addInputPath(job, new Path(examplesPath + "/" + split));
		}

		if(MavunoUtils.pathExists(conf, exampleStatsPath)) {
			FileInputFormat.addInputPath(job, new Path(exampleStatsPath));
		}

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		job.setMapOutputKeyClass(ContextPatternWritable.class);

		if("pattern".equals(splitKey)) {
			job.setSortComparatorClass(ContextPatternWritable.Comparator.class);			
		}
		else if("context".equals(splitKey)) {
			job.setSortComparatorClass(ContextPatternWritable.IdPatternComparator.class);
		}
		else if("pattern+context".equals(splitKey)) {
			job.setSortComparatorClass(ContextPatternWritable.Comparator.class);			
		}
		else {
			throw new RuntimeException("Invalid SplitKey in CombineSplits! -- " + splitKey);
		}

		job.setMapOutputValueClass(ContextPatternStatsWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(ContextPatternStatsWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);		
		return 0;
	}

}
