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

package edu.isi.mavuno.app.distsim;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.app.util.SequenceFileToText;
import edu.isi.mavuno.score.ScoreContexts;
import edu.isi.mavuno.score.ScorePatterns;
import edu.isi.mavuno.score.UpdateWeights;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class ComputePatternScores extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ComputePatternScores.class);

	public ComputePatternScores(Configuration conf) {
		super(conf);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ComputePatternScores", getConf());
		return run();
	}

	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String inputPath = MavunoUtils.getRequiredParam("Mavuno.ComputePatternScores.InputPath", conf);
		String patternScorerClass = MavunoUtils.getRequiredParam("Mavuno.ComputePatternScores.PatternScorerClass", conf);
		String patternScorerArgs = MavunoUtils.getRequiredParam("Mavuno.ComputePatternScores.PatternScorerArgs", conf);
		String contextScorerClass = MavunoUtils.getOptionalParam("Mavuno.ComputePatternScores.ContextScorerClass", conf);
		String contextScorerArgs = MavunoUtils.getOptionalParam("Mavuno.ComputePatternScores.ContextScorerArgs", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ComputePatternScores.OutputPath", conf);

		MavunoUtils.createDirectory(conf, outputPath);

		sLogger.info("Tool name: ComputePatternScores");
		sLogger.info(" - Input path: " + inputPath);
		sLogger.info(" - Pattern scorer class: " + patternScorerClass);
		sLogger.info(" - Pattern scorer args: " + patternScorerArgs);
		sLogger.info(" - Context scorer class: " + contextScorerClass);
		sLogger.info(" - Context scorer args: " + contextScorerArgs);
		sLogger.info(" - Output path: " + outputPath);

		// set total terms path
		conf.set("Mavuno.TotalTermsPath", inputPath + "/totalTerms");

		if(contextScorerClass != null) {
			// score contexts
			conf.set("Mavuno.ScoreContexts.InputPath", inputPath + "/context-stats");
			conf.set("Mavuno.ScoreContexts.OutputPath", outputPath + "/scored-contexts-raw");
			conf.set("Mavuno.Scorer.Class", contextScorerClass);
			conf.set("Mavuno.Scorer.Args", contextScorerArgs);
			new ScoreContexts(conf).run();

			// update context weights
			conf.set("Mavuno.UpdateWeights.StatsPath", inputPath + "/pattern-stats");
			conf.set("Mavuno.UpdateWeights.ScoresPath", outputPath + "/scored-contexts-raw");
			conf.set("Mavuno.UpdateWeights.ExampleType", "context");
			conf.set("Mavuno.UpdateWeights.OutputPath", outputPath + "/pattern-stats");
			new UpdateWeights(conf).run();

			conf.set("Mavuno.ScorePatterns.InputPath", outputPath + "/pattern-stats");
		}
		else {
			conf.set("Mavuno.ScorePatterns.InputPath", inputPath + "/pattern-stats");			
		}

		// score patterns
		conf.set("Mavuno.ScorePatterns.OutputPath", outputPath + "/scored-patterns-raw");
		conf.set("Mavuno.Scorer.Class", patternScorerClass);
		conf.set("Mavuno.Scorer.Args", patternScorerArgs);
		new ScorePatterns(conf).run();

		// convert sequencefile to text
		conf.set("Mavuno.SequenceFileToText.InputPath", outputPath + "/scored-patterns-raw");
		conf.set("Mavuno.SequenceFileToText.OutputPath", outputPath + "/scored-patterns");
		new SequenceFileToText(conf).run();

		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new ComputePatternScores(conf), args);
		System.exit(res);
	}

}
