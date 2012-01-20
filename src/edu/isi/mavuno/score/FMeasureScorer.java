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
public class FMeasureScorer extends Scorer {

	// tradeoff between precision and recall
	private float mLambda = 0.5f;
	
	public FMeasureScorer() {
		super();
	}

	@Override
	public void setup(Configuration conf) throws IOException {
		String paramSpec = conf.get("Mavuno.Scorer.Args", null);
		if(paramSpec != null) {
			String [] params = paramSpec.split(":");

			if(params.length != 1) {
				throw new RuntimeException("Invalid FMeasureScorer arguments --" + Arrays.toString(params));
			}

			mLambda = Float.parseFloat(params[0]);
		}
	}

	@Override
	public void scorePattern(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		result.score = stats.matches > 0 ? stats.weight : 0;
		result.staticScore = (double)stats.globalPatternCount;
		result.norm = 0.0;
	}

	@Override
	public void scoreContext(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		result.score = stats.matches > 0 ? stats.weight : 0;
		result.staticScore = (double)stats.globalContextCount;
		result.norm = 0.0;
	}

	@Override
	public double finalizeScore(double matchScore, double staticScore, double norm, int numPatterns, double totalPatternWeight) {
		return numPatterns * (matchScore/totalPatternWeight) / ((1.0-mLambda)*staticScore + mLambda*numPatterns);
	}
}
