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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.score.GetTopResults;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.TextLongPairWritable;
import edu.umd.cloud9.util.map.HMapKL;
import edu.umd.cloud9.util.map.MapKL.Entry;

/**
 * @author metzler
 *
 */
public class HarvestParaphraseCandidates extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(HarvestParaphraseCandidates.class);

	public HarvestParaphraseCandidates(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<ContextPatternWritable, LongWritable, ContextPatternWritable, TextLongPairWritable> {

		private final ContextPatternWritable mKey = new ContextPatternWritable();
		private final TextLongPairWritable mValue = new TextLongPairWritable();

		@Override
		public void map(ContextPatternWritable key, LongWritable count, Mapper<ContextPatternWritable, LongWritable, ContextPatternWritable, TextLongPairWritable>.Context context) throws IOException, InterruptedException {
			mValue.left.set(key.getPattern());
			mValue.right.set(count.get());

			mKey.set(key);
			mKey.getPattern().clear();
						
			context.write(mKey, mValue);
		}		

	}

	private static class MyReducer extends Reducer<ContextPatternWritable, TextLongPairWritable, ContextPatternWritable, DoubleWritable> {

		private final ContextPatternWritable mKey = new ContextPatternWritable();
		private final DoubleWritable mValue = new DoubleWritable();
		
		private final HMapKL<Text> mCounts = new HMapKL<Text>();
		
		@Override
		public void reduce(ContextPatternWritable key, Iterable<TextLongPairWritable> values, Reducer<ContextPatternWritable, TextLongPairWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			mCounts.clear();

			long totalCount = 0;
			for(TextLongPairWritable v : values) {
				long count = v.right.get();
				mCounts.increment(new Text(v.left), count);
				totalCount += count;
			}
			
			for(Entry<Text> entryA : mCounts.entrySet()) {
				Text a = entryA.getKey();
				double scoreA = (double)entryA.getValue() / (double)totalCount;
				mKey.setId(a);
				mKey.setContext(a);

				for(Entry<Text> entryB : mCounts.entrySet()) {
					Text b = entryB.getKey();
					double scoreB = (double)entryB.getValue() / (double)totalCount;
					
					//if(a.compareTo(b) < 0) {
						mKey.setPattern(b);
						
						mValue.set(scoreA * scoreB);
						context.write(mKey, mValue);
					//}
				}
			}
			
		}
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.HarvestParaphraseCandidates", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.HarvestParaphraseCandidates.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.HarvestParaphraseCandidates.CorpusClass", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.HarvestParaphraseCandidates.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.HarvestParaphraseCandidates.ExtractorArgs", conf);
		String numResults = MavunoUtils.getRequiredParam("Mavuno.HarvestParaphraseCandidates.NumResults", conf);
		String minMatches = MavunoUtils.getRequiredParam("Mavuno.HarvestParaphraseCandidates.MinMatches", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestParaphraseCandidates.OutputPath", conf);

		MavunoUtils.createDirectory(conf, outputPath);

		sLogger.info("Tool name: HarvestParaphraseCandidates");
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Extractor class: " + extractorClass);
		sLogger.info(" - Extractor args: " + extractorArgs);
		sLogger.info(" - Min matches: " + minMatches);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("HarvestParaphraseCandidates");

		// harvest all (context, pattern) triples
		conf.set("Mavuno.HarvestContextPatternPairs.CorpusPath", corpusPath);
		conf.set("Mavuno.HarvestContextPatternPairs.CorpusClass", corpusClass);
		conf.set("Mavuno.HarvestContextPatternPairs.ExtractorClass", extractorClass);
		conf.set("Mavuno.HarvestContextPatternPairs.ExtractorArgs", extractorArgs);
		conf.set("Mavuno.HarvestContextPatternPairs.MinMatches", minMatches);
		conf.set("Mavuno.HarvestContextPatternPairs.OutputPath", outputPath + "/triples");
		new HarvestContextPatternPairs(conf).run();
		
		FileInputFormat.addInputPath(job, new Path(outputPath + "/triples"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/patterns-all"));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		job.setPartitionerClass(ContextPatternWritable.IdContextPartitioner.class);
		job.setMapOutputValueClass(TextLongPairWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);		

		// combine scores
//		conf.set("Mavuno.CombineScores.InputPath", outputPath + "/patterns-all");
//		conf.set("Mavuno.CombineScores.OutputPath", outputPath + "/patterns");
//		new CombineScores(conf).run();
//				
		// only retain the top paraphrases
		conf.set("Mavuno.GetTopResults.InputPath", outputPath + "/patterns-all");
		conf.set("Mavuno.GetTopResults.OutputPath", outputPath + "/top-k");
		conf.set("Mavuno.GetTopResults.NumResults", numResults);
		conf.setBoolean("Mavuno.GetTopResults.SequenceFileOutputFormat", false);
		new GetTopResults(conf).run();
		
		MavunoUtils.removeDirectory(conf, outputPath + "/patterns-all");

		return 0;
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new HarvestParaphraseCandidates(conf), args);
		System.exit(res);
	}

}
