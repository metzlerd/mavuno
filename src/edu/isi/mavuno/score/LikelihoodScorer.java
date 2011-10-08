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

import edu.isi.mavuno.util.ScoreWritable;
import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;

/**
 * @author metzler
 *
 */
public class LikelihoodScorer extends Scorer {

	public LikelihoodScorer() {
		super();
	}

	@Override
	public void setup(Configuration conf) throws IOException {
		String paramSpec = conf.get("Mavuno.Scorer.Args", null);
		if(paramSpec != null && paramSpec.length() > 0) {
			throw new RuntimeException("Invalid LikelihoodScorer arguments --" + paramSpec);
		}
	}

	@Override
	public void scorePattern(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		double prob = (double)stats.matches/(double)stats.globalContextCount;
		
		result.score = stats.weight * prob;
		result.staticScore = 0.0;
		result.norm = 0.0;
	}

	@Override
	public void scoreContext(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		double prob = (double)stats.matches/(double)stats.globalPatternCount;

		result.score = stats.weight * prob;
		result.staticScore = 0.0;
		result.norm = 0.0;
	}

	@Override
	public double finalizeScore(double matchScore, double staticScore, double norm, int numPatterns, double totalPatternWeight) {
		return matchScore / totalPatternWeight;
	}
}
