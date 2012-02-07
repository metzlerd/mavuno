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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import edu.isi.mavuno.util.MavunoUtils;


/**
 * @author metzler
 *
 */
public class CombineScores extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(CombineScores.class);

	public CombineScores(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private final Text mKey = new Text();
		private final DoubleWritable mValue = new DoubleWritable();

		private final StringBuffer mBuffer = new StringBuffer();

		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
			String [] cols = value.toString().split("\t");
			
			mBuffer.setLength(0);
			for(int i = 0; i < cols.length-1; i++) {
				mBuffer.append(cols[i]);
				if(i != cols.length - 2) {
					mBuffer.append('\t');
				}
			}
			
			mKey.set(mBuffer.toString());
			mValue.set(Double.parseDouble(cols[cols.length-1]));

			context.write(mKey, mValue);
		}
	
	}

	private static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private final DoubleWritable mValue = new DoubleWritable();
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {			
			double score = 0.0;
			for(DoubleWritable v : values) {
				score += v.get();
			}

			mValue.set(score);
			context.write(key, mValue);
		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.CombineScores", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String inputPath = MavunoUtils.getRequiredParam("Mavuno.CombineScores.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.CombineScores.OutputPath", conf);

		sLogger.info("Tool name: CombineScores");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("CombineScores");
		job.setJarByClass(CombineScores.class);

		MavunoUtils.recursivelyAddInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);		

		return 0;
	}

}
