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
public class ContextToContext extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ContextToContext.class);

	public ContextToContext(Configuration conf) {
		super(conf);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ContextToContext", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String contextPath = MavunoUtils.getRequiredParam("Mavuno.ContextToContext.ContextPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.ContextToContext.CorpusClass", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.ContextToContext.CorpusPath", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.ContextToContext.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.ContextToContext.ExtractorArgs", conf);
		int minMatches = Integer.parseInt(MavunoUtils.getRequiredParam("Mavuno.ContextToContext.MinMatches", conf));
		boolean harvestGlobalStats = Boolean.parseBoolean(MavunoUtils.getRequiredParam("Mavuno.ContextToContext.GlobalStats", conf));
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ContextToContext.OutputPath", conf);

		MavunoUtils.createDirectory(conf, outputPath);

		sLogger.info("Tool name: ContextToContext");
		sLogger.info(" - Context path: " + contextPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Extractor class: " + extractorClass);
		sLogger.info(" - Extractor arguments: " + extractorArgs);
		sLogger.info(" - Min matches: " + minMatches);
		sLogger.info(" - Harvest global stats: " + harvestGlobalStats);
		
		// context to pattern
		conf.set("Mavuno.ContextToPattern.ContextPath", contextPath);
		conf.set("Mavuno.ContextToPattern.CorpusPath", corpusPath);
		conf.set("Mavuno.ContextToPattern.CorpusClass", corpusClass);
		conf.set("Mavuno.ContextToPattern.ExtractorClass", extractorClass);
		conf.set("Mavuno.ContextToPattern.ExtractorArgs", extractorArgs);
		conf.setInt("Mavuno.ContextToPattern.MinMatches", minMatches);
		conf.setBoolean("Mavuno.ContextToPattern.GlobalStats", harvestGlobalStats);
		conf.set("Mavuno.ContextToPattern.OutputPath", outputPath);
		new ContextToPattern(conf).run();

		// pattern to context
		conf.set("Mavuno.PatternToContext.PatternPath", outputPath + "/pattern-stats");
		conf.set("Mavuno.PatternToContext.CorpusPath", corpusPath);
		conf.set("Mavuno.PatternToContext.CorpusClass", corpusClass);
		conf.set("Mavuno.PatternToContext.ExtractorClass", extractorClass);
		conf.set("Mavuno.PatternToContext.ExtractorArgs", extractorArgs);
		conf.setInt("Mavuno.PatternToContext.MinMatches", minMatches);
		conf.setBoolean("Mavuno.PatternToContext.GlobalStats", harvestGlobalStats);
		conf.set("Mavuno.PatternToContext.OutputPath", outputPath);
		new PatternToContext(conf).run();

		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new ContextToContext(conf), args);
		System.exit(res);
	}

}
