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

import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class PatternToPattern extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(PatternToPattern.class);

	public PatternToPattern(Configuration conf) {
		super(conf);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.PatternToPattern", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();
		
		String patternPath = MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.PatternPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.CorpusClass", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.CorpusPath", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.ExtractorArgs", conf);
		int minMatches = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.MinMatches", conf));
		boolean harvestGlobalStats = Boolean.parseBoolean(MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.GlobalStats", conf));
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.PatternToPattern.OutputPath", conf);

		MavunoUtils.createDirectory(conf, outputPath);

		sLogger.info("Tool name: PatternToPattern");
		sLogger.info(" - Pattern path: " + patternPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Context class: " + extractorClass);
		sLogger.info(" - Context arguments: " + extractorArgs);
		sLogger.info(" - Min matches: " + minMatches);
		sLogger.info(" - Harvest global stats: " + harvestGlobalStats);

		// pattern to context
		conf.set("Mavuno.PatternToContext.PatternPath", patternPath);
		conf.set("Mavuno.PatternToContext.CorpusPath", corpusPath);
		conf.set("Mavuno.PatternToContext.CorpusClass", corpusClass);
		conf.set("Mavuno.PatternToContext.ExtractorClass", extractorClass);
		conf.set("Mavuno.PatternToContext.ExtractorArgs", extractorArgs);
		conf.setInt("Mavuno.PatternToContext.MinMatches", minMatches);
		conf.setBoolean("Mavuno.PatternToContext.GlobalStats", harvestGlobalStats);
		conf.set("Mavuno.PatternToContext.OutputPath", outputPath);
		new PatternToContext(conf).run();

		// context to pattern
		conf.set("Mavuno.ContextToPattern.ContextPath", outputPath + "/context-stats");
		conf.set("Mavuno.ContextToPattern.CorpusPath", corpusPath);
		conf.set("Mavuno.ContextToPattern.CorpusClass", corpusClass);
		conf.set("Mavuno.ContextToPattern.ExtractorClass", extractorClass);
		conf.set("Mavuno.ContextToPattern.ExtractorArgs", extractorArgs);
		conf.setInt("Mavuno.ContextToPattern.MinMatches", minMatches);
		conf.setBoolean("Mavuno.ContextToPattern.GlobalStats", harvestGlobalStats);
		conf.set("Mavuno.ContextToPattern.OutputPath", outputPath);
		new ContextToPattern(conf).run();
				
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new PatternToPattern(conf), args);
		System.exit(res);
	}

}
