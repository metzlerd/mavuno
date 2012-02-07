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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import edu.isi.mavuno.util.ScoreWritable;
import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class ScoreContexts extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(ScoreContexts.class);

	public ScoreContexts(Configuration conf) throws IOException {
		super(conf);
	}

	private static class MyMapper extends Mapper<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ScoreWritable> {

		// scorer
		private Scorer mScorer = null;

		// score
		private final ScoreWritable mScore = new ScoreWritable();

		// text
		private final Text mText = new Text();
		
		@Override
		public void setup(Mapper<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ScoreWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			try {
				// get the global stats path
				String globalStatsPath = conf.get("Mavuno.TotalTermsPath", null);

				// initialize the scorer
				mScorer = (Scorer)Class.forName(conf.get("Mavuno.Scorer.Class")).newInstance();
				mScorer.setup(conf);

				// get the total number of contexts in the corpus (if applicable)
				if(MavunoUtils.pathExists(conf, globalStatsPath)) {
					long totalTerms = MavunoUtils.getTotalTerms(conf, globalStatsPath);
					mScorer.setTotalTerms(totalTerms);
				}
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(ContextPatternWritable key, ContextPatternStatsWritable value, Mapper<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ScoreWritable>.Context context) throws IOException, InterruptedException {
			// score context with respect to context
			mScorer.scoreContext(key, value, mScore);

			// temporarily store the pattern
			mText.set(key.getPattern());

			// context score
			key.setPattern(ContextPatternWritable.ASTERISK);
			context.write(key, mScore);
			
			// id score
			key.setContext(ContextPatternWritable.ASTERISK);
			context.write(key, mScore);
			
			// pattern score
			key.setPattern(mText);
			mScore.staticScore = value.weight; // use the static score to store the pattern's weight
			context.write(key, mScore);
		}

		@Override
		public void cleanup(Mapper<ContextPatternWritable, ContextPatternStatsWritable, ContextPatternWritable, ScoreWritable>.Context context) throws IOException, InterruptedException {
			mScorer.cleanup(context.getConfiguration());
		}
	}

	private static class MyReducer extends Reducer<ContextPatternWritable, ScoreWritable, ContextPatternWritable, DoubleWritable> {

		private final DoubleWritable mResult = new DoubleWritable();

		// the last pattern we observed
		private final Text mLastPattern = new Text();
		
		// score normalization factor
		private double mNorm;

		// pattern stats
		private int mNumPatterns = 0;
		private double mTotalPatternWeight = 0.0;
		
		// scorer
		private Scorer mScorer = null;

		@Override
		public void setup(Reducer<ContextPatternWritable, ScoreWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			try {
				// initialize the scorer
				mScorer = (Scorer)Class.forName(conf.get("Mavuno.Scorer.Class")).newInstance();
				mScorer.setup(conf);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void reduce(ContextPatternWritable key, Iterable<ScoreWritable> values, Reducer<ContextPatternWritable, ScoreWritable, ContextPatternWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
			double totalNorm = 0.0;

			// get non-normalized score
			double contextScore = 0.0;
			double staticScore = 0.0;
			for(ScoreWritable value : values) {
				contextScore = mScorer.updateScore(contextScore, value.score);
				staticScore = value.staticScore; // this should be equal across all patterns for a given context
				totalNorm = mScorer.updateNorm(totalNorm, value.norm);
			}

			if(key.getPattern().equals(ContextPatternWritable.ASTERISK) && key.getContext().equals(ContextPatternWritable.ASTERISK)) {
				mNorm = totalNorm;

				mLastPattern.clear();
				mNumPatterns = 0;
				mTotalPatternWeight = 0.0;
				
				return;
			}

			if(key.getContext().equals(ContextPatternWritable.ASTERISK)) {
				if(!mLastPattern.equals(key.getPattern())) {
					mNumPatterns++;
					mTotalPatternWeight += staticScore;
					mLastPattern.set(key.getPattern());
				}
				return;
			}
			
			mResult.set(mScorer.finalizeScore(contextScore, staticScore, mNorm, mNumPatterns, mTotalPatternWeight));

			context.write(key, mResult);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ScoreContexts", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.ScoreContexts.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ScoreContexts.OutputPath", conf);
		String contextScorerClass = MavunoUtils.getRequiredParam("Mavuno.Scorer.Class", conf);
		String contextScorerArgs = MavunoUtils.getRequiredParam("Mavuno.Scorer.Args", conf);

		sLogger.info("Tool name: ScoreContexts");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Context scorer class: " + contextScorerClass);
		sLogger.info(" - Context scorer args: " + contextScorerArgs);

		Job job = new Job(conf);
		job.setJobName("ScoreContexts");
		job.setJarByClass(ScoreContexts.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setSortComparatorClass(ContextPatternWritable.Comparator.class);
		job.setPartitionerClass(ContextPatternWritable.IdPartitioner.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		job.setMapOutputKeyClass(ContextPatternWritable.class);
		job.setMapOutputValueClass(ScoreWritable.class);

		job.setOutputKeyClass(ContextPatternWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);		

		return 0;
	}
}