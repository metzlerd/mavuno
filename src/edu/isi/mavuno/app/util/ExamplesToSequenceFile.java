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

package edu.isi.mavuno.app.util;

import java.io.IOException;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class ExamplesToSequenceFile extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ExamplesToSequenceFile.class);

	public ExamplesToSequenceFile(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<LongWritable, Text, ContextPatternWritable, DoubleWritable> {
		
		private final ContextPatternWritable mContext = new ContextPatternWritable();
		private final DoubleWritable mCount = new DoubleWritable();
		
		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			String [] cols = value.toString().split("\t");
			
			mContext.setId(cols[ContextPatternWritable.ID_FIELD]);
			mContext.setContext(cols[ContextPatternWritable.CONTEXT_FIELD]);
			mContext.setPattern(cols[ContextPatternWritable.PATTERN_FIELD]);
			
			mCount.set(Double.parseDouble(cols[ContextPatternWritable.TOTAL_FIELDS]));
			
			context.write(mContext, mCount);
		}
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, DoubleWritable, ContextPatternWritable, DoubleWritable> {
		/* identity reducer */
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ExamplesToSequenceFile", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String contextPath = MavunoUtils.getRequiredParam("Mavuno.ExamplesToSequenceFile.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ExamplesToSequenceFile.OutputPath", conf);

		sLogger.info("Tool name: ExamplesToSequenceFile");
		sLogger.info(" - Context path: " + contextPath);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("ExamplesToSequenceFile");
		job.setJarByClass(ExamplesToSequenceFile.class);

		FileInputFormat.addInputPath(job, new Path(contextPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		job.setPartitionerClass(ContextPatternWritable.FullPartitioner.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.waitForCompletion(true);		
		return 0;
	}

	public static void main(String [] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new ExamplesToSequenceFile(conf), args);
		System.exit(res);
	}
	
}
