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
public class ScorePatterns extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(ScorePatterns.class);

	public ScorePatterns(Configuration conf) throws IOException {
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

				// get the total number of terms in the corpus (if applicable)
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
			// score pattern with respect to context
			mScorer.scorePattern(key, value, mScore);

			// temporarily store the context
			mText.set(key.getContext());

			// pattern score
			key.setContext(ContextPatternWritable.ASTERISK);
			context.write(key, mScore);				

			// id score
			key.setPattern(ContextPatternWritable.ASTERISK);
			context.write(key, mScore);

			// context score
			key.setContext(mText);
			mScore.staticScore = value.weight; // use the static score to store the context's weight
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
		private final Text mLastContext = new Text();

		// normalization factor
		private double mNorm;

		// pattern stats
		private int mNumContexts = 0;
		private double mTotalContextWeight = 0.0;

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
			double patternScore = 0.0;
			double staticScore = 0.0;
			for(ScoreWritable value : values) {
				patternScore = mScorer.updateScore(patternScore, value.score);
				staticScore = value.staticScore; // this should be equal across all contexts for a given pattern
				totalNorm = mScorer.updateNorm(totalNorm, value.norm);
			}

			if(key.getPattern().equals(ContextPatternWritable.ASTERISK) && key.getContext().equals(ContextPatternWritable.ASTERISK)) {
				mNorm = totalNorm;

				mLastContext.clear();
				mNumContexts = 0;
				mTotalContextWeight = 0.0;

				return;
			}

			if(key.getPattern().equals(ContextPatternWritable.ASTERISK)) {
				if(!mLastContext.equals(key.getContext())) {
					mNumContexts++;
					mTotalContextWeight += staticScore;
					mLastContext.set(key.getContext());
				}
				return;
			}

			mResult.set(mScorer.finalizeScore(patternScore, staticScore, mNorm, mNumContexts, mTotalContextWeight));

			context.write(key, mResult);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ScorePatterns", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.ScorePatterns.InputPath", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ScorePatterns.OutputPath", conf);
		String patternScorerClass = MavunoUtils.getRequiredParam("Mavuno.Scorer.Class", conf);
		String patternScorerArgs = MavunoUtils.getRequiredParam("Mavuno.Scorer.Args", conf);

		sLogger.info("Tool name: ScorePatterns");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Pattern scorer class: " + patternScorerClass);
		sLogger.info(" - Pattern scorer args: " + patternScorerArgs);

		Job job = new Job(conf);
		job.setJobName("ScorePatterns");
		job.setJarByClass(ScorePatterns.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setSortComparatorClass(ContextPatternWritable.IdPatternComparator.class);
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