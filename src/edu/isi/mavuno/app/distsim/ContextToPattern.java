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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.extract.CombineSplits;
import edu.isi.mavuno.extract.ExtractGlobalStats;
import edu.isi.mavuno.extract.Extract;
import edu.isi.mavuno.extract.Split;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class ContextToPattern extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ContextToPattern.class);

	public ContextToPattern(Configuration conf) {
		super(conf);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ContextToPattern", getConf());
		return run();
	}
	
	public int run() throws ClassNotFoundException, InterruptedException, IOException {		
		Configuration conf = getConf();

		String contextPath = MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.ContextPath", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.CorpusClass", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.ExtractorArgs", conf);
		String minMatches = MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.MinMatches", conf);
		boolean harvestGlobalStats = Boolean.parseBoolean(MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.GlobalStats", conf));
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ContextToPattern.OutputPath", conf);

		MavunoUtils.createDirectory(conf, outputPath);

		sLogger.info("Tool name: ContextToPattern");
		sLogger.info(" - Context path: " + contextPath);
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Extractor class: " + extractorClass);
		sLogger.info(" - Extractor args: " + extractorArgs);
		sLogger.info(" - Min matches: " + minMatches);
		sLogger.info(" - Harvest global stats: " + harvestGlobalStats);
		sLogger.info(" - Output path: " + outputPath);

		// set total terms path
		conf.set("Mavuno.TotalTermsPath", outputPath + "/totalTerms");

		// split contexts into manageable chunks
		conf.set("Mavuno.Split.InputPath", contextPath);
		conf.set("Mavuno.Split.OutputPath", outputPath + "/contexts-split");
		conf.set("Mavuno.Split.SplitKey", "context");
		new Split(conf).run();

		// get context splits
		FileStatus [] files = MavunoUtils.getDirectoryListing(conf, outputPath + "/contexts-split");
		int split = 0;
		for(FileStatus file : files) {
			if(!file.getPath().getName().endsWith(".examples")) {
				continue;
			}

			// extract patterns
			conf.set("Mavuno.Extract.InputPath", file.getPath().toString());
			conf.set("Mavuno.Extract.CorpusPath", corpusPath);
			conf.set("Mavuno.Extract.CorpusClass", corpusClass);
			conf.set("Mavuno.Extract.ExtractorClass", extractorClass);
			conf.set("Mavuno.Extract.ExtractorArgs", extractorArgs);
			conf.set("Mavuno.Extract.ExtractorTarget", "pattern");
			conf.set("Mavuno.Extract.MinMatches", minMatches);
			conf.set("Mavuno.Extract.OutputPath", outputPath + "/contexts-split/patterns/" + split);
			new Extract(conf).run();

			// increment split
			split++;
		}

		// extract global pattern statistics if necessary
		if(harvestGlobalStats) {
			conf.set("Mavuno.ExtractGlobalStats.InputPath", outputPath + "/contexts-split/patterns/");
			conf.set("Mavuno.ExtractGlobalStats.CorpusPath", corpusPath);
			conf.set("Mavuno.ExtractGlobalStats.CorpusClass", corpusClass);
			conf.set("Mavuno.ExtractGlobalStats.ExtractorClass", extractorClass);
			conf.set("Mavuno.ExtractGlobalStats.ExtractorArgs", extractorArgs);
			conf.set("Mavuno.ExtractGlobalStats.ExtractorTarget", "pattern");
			conf.set("Mavuno.ExtractGlobalStats.OutputPath", outputPath + "/contexts-split/pattern-stats/");
			new ExtractGlobalStats(conf).run();
		}

		// combine context splits
		conf.set("Mavuno.CombineSplits.ExamplesPath", outputPath + "/contexts-split/patterns");
		conf.set("Mavuno.CombineSplits.ExampleStatsPath", outputPath + "/contexts-split/pattern-stats");
		conf.set("Mavuno.CombineSplits.SplitKey", "context");
		conf.setInt("Mavuno.CombineSplits.TotalSplits", split);
		conf.set("Mavuno.CombineSplits.OutputPath", outputPath + "/pattern-stats");
		new CombineSplits(conf).run();

		// delete context splits
		MavunoUtils.removeDirectory(conf, outputPath + "/contexts-split");

		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new ContextToPattern(conf), args);
		System.exit(res);
	}

}
