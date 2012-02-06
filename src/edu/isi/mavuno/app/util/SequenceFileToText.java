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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.util.MavunoUtils;


/**
 * @author metzler
 *
 */
public class SequenceFileToText extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(SequenceFileToText.class);

	public SequenceFileToText(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Writable, Text, Text> {

		private final Text mKey = new Text();
		private final Text mValue = new Text();
		
		@Override
		public void map(Writable key, Writable value, Mapper<Writable, Writable, Text, Text>.Context context) throws IOException, InterruptedException {			
			mKey.set(key.toString());
			mValue.set(value.toString());
			context.write(mKey, mValue);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {		
		MavunoUtils.readParameters(args, "Mavuno.SequenceFileToText", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String inputPath = MavunoUtils.getRequiredParam("Mavuno.SequenceFileToText.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.SequenceFileToText.OutputPath", conf);

		sLogger.info("Tool name: SequenceFileToText");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("SequenceFileToText");
		job.setJarByClass(SequenceFileToText.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);
		
		job.waitForCompletion(true);		
		return 0;
	}

	public static void main(String [] args) throws Exception {
		Configuration conf = new Configuration();		
		int res = ToolRunner.run(new SequenceFileToText(conf), args);
		System.exit(res);
	}
	
}
