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
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import edu.isi.mavuno.util.ScoreWritable;
import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;

/**
 * @author metzler
 *
 */
public class PMIScorer extends Scorer {

	private enum Weighting { PMI, EMIM };

	private Weighting mWeightType;
	private int mSmoothCount = 0;

	public PMIScorer() {
		super();
	}

	@Override
	public void setup(Configuration conf) throws IOException {
		String paramSpec = conf.get("Mavuno.Scorer.Args", null);
		if(paramSpec != null) {
			String [] params = paramSpec.split(":");

			if(params.length == 0 || params.length > 2) {
				throw new RuntimeException("Invalid PMIScorer arguments --" + Arrays.toString(params));
			}

			String weightType = params[0].toLowerCase().trim();
			if("pmi".equals(weightType)) {
				mWeightType = Weighting.PMI;
			}
			else if("emim".equals(weightType)) {
				mWeightType = Weighting.EMIM;
			}
			else {
				throw new RuntimeException("Invalid weight type -- " + params[0]);
			}

			if(params.length == 2) {
				mSmoothCount = Integer.parseInt(params[1]);
			}
		}
	}

	@Override
	public void scorePattern(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		// get background probability
		long totalTerms = getTotalTerms();
		double backgroundProb = (double)stats.globalPatternCount/(double)totalTerms;

		// compute score
		double score = getScore(stats, backgroundProb);	

		result.score = stats.weight * score;
		result.staticScore = 0.0;
		result.norm = 0.0;
	}

	@Override
	public void scoreContext(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		// get background probability
		long totalTerms = getTotalTerms();
		double backgroundProb = (double)stats.globalContextCount/(double)totalTerms;

		// compute score
		double score = getScore(stats, backgroundProb);	

		result.score = stats.weight * score;
		result.staticScore = 0.0;
		result.norm = 0.0;
	}

	private double getScore(ContextPatternStatsWritable stats, double backgroundProb) {
		// get the total number of terms and total domain score
		long totalTerms = getTotalTerms();

		// (weighted) pmi(pattern, context)
		return getPMI(stats.matches, stats.globalPatternCount + mSmoothCount, stats.globalContextCount + mSmoothCount, totalTerms);
	}

	private double getPMI(double xy, double x, double y, long totalTerms) {
		if(xy == 0.0 || x == 0.0 || y == 0.0) {
			return 0.0;
		}

		if(mWeightType == Weighting.PMI) {
			return Math.log(xy * totalTerms / (x * y));
		}
		else if(mWeightType == Weighting.EMIM) {
			return (xy / totalTerms) * Math.log(xy * totalTerms / (x * y));
		}

		return 0.0; // should never happen
	}

	@Override
	public double finalizeScore(double matchScore, double staticScore, double norm, int numPatterns, double totalPatternWeight) {
		return matchScore / totalPatternWeight;
	}
}
