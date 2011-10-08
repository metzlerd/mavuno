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

package edu.isi.mavuno.app.ie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.app.distsim.ComputeContextScores;
import edu.isi.mavuno.app.distsim.ComputePatternScores;
import edu.isi.mavuno.app.distsim.ContextToPattern;
import edu.isi.mavuno.app.distsim.PatternToContext;
import edu.isi.mavuno.app.util.ExamplesToSequenceFile;
import edu.isi.mavuno.score.GetTopResults;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class HarvestEspressoContexts extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(HarvestEspressoContexts.class);

	public HarvestEspressoContexts(Configuration conf) {
		super(conf);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.HarvestEspressoContexts", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String inputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.InputPath", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.CorpusClass", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.ExtractorArgs", conf);
		String scorerClass = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.ScorerClass", conf);
		String scorerArgs = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.ScorerArgs", conf);
		int numPatterns = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.NumPatterns", conf));
		int minMatches = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.MinMatches", conf));
		String baseOutputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestEspressoContexts.OutputPath", conf);

		String numIterations = MavunoUtils.getOptionalParam("Mavuno.HarvestEspressoContexts.NumIterations", conf);
		int iterations = 1;
		if(numIterations != null){
			iterations = Integer.parseInt(numIterations);
		}

		MavunoUtils.createDirectory(conf, baseOutputPath);

		sLogger.info("Tool name: HarvestEspressoContexts");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Extractor class: " + extractorClass);
		sLogger.info(" - Extractor args: " + extractorArgs);
		sLogger.info(" - Scorer class: " + scorerClass);
		sLogger.info(" - Scorer args: " + scorerArgs);
		sLogger.info(" - Number of patterns: " + numPatterns);
		sLogger.info(" - Minimum matches: " + minMatches);
		sLogger.info(" - Iterations: " + iterations);
		sLogger.info(" - Output path: " + baseOutputPath);

		// initial sub output path
		MavunoUtils.createDirectory(conf, baseOutputPath + "/0");
		MavunoUtils.createDirectory(conf, baseOutputPath + "/0/contexts-scored");
		
		// examples -> sequence file
		conf.set("Mavuno.ExamplesToSequenceFile.InputPath", inputPath);
		conf.set("Mavuno.ExamplesToSequenceFile.OutputPath", baseOutputPath + "/0/contexts-scored/scored-contexts-raw");
		new ExamplesToSequenceFile(conf).run();

		// iterate procedure
		for(int i = 1; i <= iterations; i++) {
			// previous output path (input to current iteration)
			String prevOutputPath = baseOutputPath + "/" + (i-1);

			// current output path
			String curOutputPath = baseOutputPath + "/" + i;
			MavunoUtils.createDirectory(conf, curOutputPath);

			// seeds -> patterns
			conf.set("Mavuno.ContextToPattern.ContextPath", prevOutputPath + "/contexts-scored/scored-contexts-raw");
			conf.set("Mavuno.ContextToPattern.CorpusPath", corpusPath);
			conf.set("Mavuno.ContextToPattern.CorpusClass", corpusClass);
			conf.set("Mavuno.ContextToPattern.ExtractorClass", extractorClass);
			conf.set("Mavuno.ContextToPattern.ExtractorArgs", extractorArgs);
			conf.setInt("Mavuno.ContextToPattern.MinMatches", minMatches);
			conf.setBoolean("Mavuno.ContextToPattern.GlobalStats", true);
			conf.set("Mavuno.ContextToPattern.OutputPath", curOutputPath + "/patterns");
			new ContextToPattern(conf).run();

			// score patterns
			conf.set("Mavuno.ComputePatternScores.InputPath", curOutputPath + "/patterns");
			conf.set("Mavuno.ComputePatternScores.ContextScorerClass", null);
			conf.set("Mavuno.ComputePatternScores.PatternScorerClass", scorerClass);
			conf.set("Mavuno.ComputePatternScores.PatternScorerArgs", scorerArgs);
			conf.set("Mavuno.ComputePatternScores.OutputPath", curOutputPath + "/patterns-scored");
			new ComputePatternScores(conf).run();

			// only retain top-(k * i) patterns
			if(numPatterns >= 0) {
				conf.set("Mavuno.GetTopResults.InputPath", curOutputPath + "/patterns-scored/scored-patterns");
				conf.set("Mavuno.GetTopResults.OutputPath", curOutputPath + "/patterns-scored-top");
				conf.setInt("Mavuno.GetTopResults.NumResults", numPatterns * i);
				conf.setBoolean("Mavuno.GetTopResults.SequenceFileOutputFormat", true);
				new GetTopResults(conf).run();
			}

			// patterns -> contexts
			if(numPatterns >= 0) {
				conf.set("Mavuno.PatternToContext.PatternPath", curOutputPath + "/patterns-scored-top");
			}
			else {
				conf.set("Mavuno.PatternToContext.PatternPath", curOutputPath + "/patterns-scored/scored-patterns-raw");				
			}
			conf.set("Mavuno.PatternToContext.CorpusPath", corpusPath);
			conf.set("Mavuno.PatternToContext.CorpusClass", corpusClass);
			conf.set("Mavuno.PatternToContext.ExtractorClass", extractorClass);
			conf.set("Mavuno.PatternToContext.ExtractorArgs", extractorArgs);
			conf.setInt("Mavuno.PatternToContext.MinMatches", minMatches);
			conf.setBoolean("Mavuno.PatternToContext.GlobalStats", true);
			conf.set("Mavuno.PatternToContext.OutputPath", curOutputPath + "/contexts");
			new PatternToContext(conf).run();

			// score contexts
			conf.set("Mavuno.ComputeContextScores.InputPath", curOutputPath + "/contexts");
			conf.set("Mavuno.ComputeContextScores.PatternScorerClass", null);
			conf.set("Mavuno.ComputeContextScores.ContextScorerClass", scorerClass);
			conf.set("Mavuno.ComputeContextScores.ContextScorerArgs", scorerArgs);
			conf.set("Mavuno.ComputeContextScores.OutputPath", curOutputPath + "/contexts-scored");
			new ComputeContextScores(conf).run();

			// delete previous output path
			MavunoUtils.removeDirectory(conf, prevOutputPath);
		}

		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new HarvestEspressoContexts(conf), args);
		System.exit(res);
	}

}
