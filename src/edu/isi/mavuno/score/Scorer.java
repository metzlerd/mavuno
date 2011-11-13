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
public abstract class Scorer {
	
	// global statistics
	private long mTotalTerms = 0L;
	
	public void setup(Configuration conf) throws IOException {
	}
	
	public void cleanup(Configuration conf) throws IOException {
	}

	public long getTotalTerms() {
		return mTotalTerms;
	}
	
	public final void setTotalTerms(long totalTerms) {
		mTotalTerms = totalTerms;
	}

	// scores a pattern
	public abstract void scorePattern(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result);

	// scores a context
	public abstract void scoreContext(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result);

	// update the score (override to implement other types of score updates)
	public double updateScore(double curTotalScore, double score) {
		return curTotalScore + score;
	}

	// update the norm (override to implement other types of norm updates)
	public double updateNorm(double curTotalNorm, double norm) {
		return curTotalNorm + norm;
	}

	// finalize score
	public abstract double finalizeScore(double matchScore, double staticScore, double norm, int numPatterns, double totalPatternWeight);
}
