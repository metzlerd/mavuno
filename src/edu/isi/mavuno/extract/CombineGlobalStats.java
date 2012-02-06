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
public class CombineGlobalStats extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(CombineGlobalStats.class);

	public CombineGlobalStats(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {
		/* identity mapper */
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable> {

		private final ContextPatternStatsWritable mCountScorePair = new ContextPatternStatsWritable();
		
		@Override
		public void reduce(ContextPatternWritable key, Iterable<ContextPatternStatsWritable> values, Reducer<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ContextPatternStatsWritable>.Context context) throws IOException, InterruptedException {
			mCountScorePair.zero();
			
			for(ContextPatternStatsWritable p : values) {
				mCountScorePair.increment(p);
			}

			context.write(key, mCountScorePair);
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.CombineGlobalStats", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.CombineGlobalStats.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.CombineGlobalStats.OutputPath", conf);		
		int numSplits = conf.getInt("Mavuno.CombineGlobalStats.TotalSplits", 1);

		sLogger.info("Tool name: CombineGlobalStats");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Number of splits: " + numSplits);

		Job job = new Job(conf);
		job.setJobName("CombineGlobalStats");
		job.setJarByClass(CombineGlobalStats.class);

		for(int split = 0; split < numSplits; split++) {
			FileInputFormat.addInputPath(job, new Path(inputPath + "/" + split));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		job.setMapOutputValueClass(ContextPatternStatsWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(ContextPatternStatsWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);		

		return 0;
	}

}
